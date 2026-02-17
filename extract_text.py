"""从 EPUB/PDF 提取文本并保存为 Markdown 文件。支持流水线并行处理和多模型负载均衡。"""

import argparse
import base64
import io
import itertools
import json
import os
import queue
import re
import signal
import sys
import threading
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import ebooklib
from ebooklib import epub
import fitz
import markdownify
from openai import OpenAI
from PIL import Image
import pytesseract

_shutdown_flag = threading.Event()

# 全局配置，由 main() 初始化
CFG: dict = {}
SETTINGS: dict = {}
API_CFG: dict = {}
FILE_DIRS: dict = {}
API_KEY = ""
API_BASE = ""
MODELS: list[str] = []
TESS_LANG = "chi_sim+eng"
MIN_TEXT_LEN = 50
MIN_OCR_LEN = 20
MAX_WORKERS = 4
DPI = 200
SAVE_INTERVAL = 10

_model_cycle = itertools.cycle([""])
_model_lock = threading.Lock()


def _signal_handler(sig, frame):
    print("\n收到中断信号，正在优雅退出...")
    _shutdown_flag.set()


signal.signal(signal.SIGINT, _signal_handler)


def create_default_config(config_file: str):
    Path(config_file).write_text("""\
[settings]
tesseract_cmd = "D:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe"
tesseract_lang = "chi_sim+eng"
min_text_len = 50
min_ocr_len = 20
max_workers = 4
dpi = 200
save_interval = 10

[api]
key = ""
base_url = "https://api.siliconflow.cn/v1"
models = ["zai-org/GLM-4.6V"]

[file_dirs]
input_dir = "."
output_dir = "."
""", encoding="utf-8")
    print(f"已生成默认配置文件: {config_file}")
    print("请先填写 [api] key 后再运行。")
    sys.exit(0)


def load_config(config_file: str) -> dict:
    p = Path(config_file)
    if not p.exists():
        create_default_config(config_file)
    with open(p, "rb") as f:
        cfg = tomllib.load(f)
    api_key = cfg.get("api", {}).get("key", "")
    if not api_key:
        print(f"错误: 请在 {config_file} 中填写 [api] key")
        sys.exit(1)
    return cfg


def init_globals(cfg: dict, args):
    """根据配置和命令行参数初始化全局变量。"""
    global CFG, SETTINGS, API_CFG, FILE_DIRS
    global API_KEY, API_BASE, MODELS, _model_cycle
    global TESS_LANG, MIN_TEXT_LEN, MIN_OCR_LEN, MAX_WORKERS, DPI, SAVE_INTERVAL

    CFG = cfg
    SETTINGS = cfg.get("settings", {})
    API_CFG = cfg.get("api", {})
    FILE_DIRS = cfg.get("file_dirs", {})

    pytesseract.pytesseract.tesseract_cmd = SETTINGS.get(
        "tesseract_cmd", r"D:\Program Files (x86)\Tesseract-OCR\tesseract.exe"
    )
    TESS_LANG = SETTINGS.get("tesseract_lang", "chi_sim+eng")
    MIN_TEXT_LEN = SETTINGS.get("min_text_len", 50)
    MIN_OCR_LEN = SETTINGS.get("min_ocr_len", 20)
    SAVE_INTERVAL = SETTINGS.get("save_interval", 10)

    # DPI: 命令行 > 配置文件 > 默认200
    DPI = args.dpi or SETTINGS.get("dpi", 200)

    # Workers: --w-full 使用 CPU 核心数*4, -w 指定, 否则配置文件
    if args.w_full:
        MAX_WORKERS = os.cpu_count() * 4 or 32
    elif args.workers:
        MAX_WORKERS = args.workers
    else:
        MAX_WORKERS = SETTINGS.get("max_workers", 4)

    API_KEY = API_CFG.get("key", "")
    API_BASE = API_CFG.get("base_url", "https://api.siliconflow.cn/v1")

    MODELS = API_CFG.get("models", ["zai-org/GLM-4.6V"])
    if not MODELS or MODELS == ["zai-org/GLM-4.6V"]:
        single = API_CFG.get("model", "")
        if single:
            MODELS = [single]
    _model_cycle = itertools.cycle(MODELS)


def next_model() -> str:
    with _model_lock:
        return next(_model_cycle)


def parse_pages(spec: str, total: int) -> set[int]:
    if spec.strip().lower() == "all":
        return set(range(total))
    result = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            result.update(range(int(a) - 1, int(b)))
        else:
            result.add(int(part) - 1)
    return result


def get_file_config(filename: str) -> dict:
    files_list = CFG.get("files", [])
    if not files_list:
        fc_section = CFG.get("files_config", {})
        files_list = fc_section.get("files", [])
    for fc in files_list:
        if fc.get("name") == filename:
            return fc
    return {}


def get_input_dir() -> Path:
    return Path(FILE_DIRS.get("input_dir", "."))


def get_output_dir() -> Path:
    return Path(FILE_DIRS.get("output_dir", "."))


def _progress_path(filepath: Path) -> Path:
    return filepath.with_suffix(".progress.json")


def load_progress(filepath: Path) -> dict[int, str]:
    p = _progress_path(filepath)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {int(k): v for k, v in data.get("completed", {}).items()}
    except Exception:
        return {}


def save_progress(filepath: Path, results: dict[int, str]):
    p = _progress_path(filepath)
    data = {"completed": {str(k): v for k, v in results.items()}}
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def clean_markdown(text: str) -> str:
    text = re.sub(r'<\|begin_of_box\|>', '', text)
    text = re.sub(r'<\|end_of_box\|>', '', text)
    text = re.sub(r'```markdown', '', text)
    text = re.sub(r'```', '', text)
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    return text.strip()


def extract_epub(filepath: Path) -> str:
    book = epub.read_epub(str(filepath), options={"ignore_ncx": True})
    parts: list[str] = []
    for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
        html: str = item.get_content().decode("utf-8", errors="ignore")
        md = markdownify.markdownify(html, heading_style="ATX", strip=["img", "a"])
        md = re.sub(r"\n{3,}", "\n\n", md).strip()
        if md:
            parts.append(md)
    return "\n\n---\n\n".join(parts)


def ocr_tesseract(image_bytes: bytes) -> str:
    try:
        img = Image.open(io.BytesIO(image_bytes))
        return pytesseract.image_to_string(img, lang=TESS_LANG).strip()
    except Exception as e:
        print(f"  [Tesseract 失败] {e}")
        return ""


def ocr_vlm(image_bytes: bytes) -> str:
    model = next_model()
    try:
        b64 = base64.b64encode(image_bytes).decode()
        client = OpenAI(api_key=API_KEY, base_url=API_BASE, timeout=30)
        resp = client.chat.completions.create(
            model=model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    {"type": "text", "text": "请提取这张图片中的所有文字内容，保留原文的标题、段落、列表等格式，使用Markdown语法输出。仅输出提取的内容，不要有其他解释，不要输出代码块标和特殊标签。"},
                ],
            }],
            max_tokens=4096,
        )
        content = resp.choices[0].message.content.strip()
        return clean_markdown(content)
    except Exception as e:
        print(f"  [VLM API 失败 model={model}] {e}")
        return ""


def process_ocr_page(args: tuple) -> tuple[int, str]:
    idx, total, method, text, image_bytes = args
    print(f"  OCR 第 {idx + 1}/{total} 页 [method={method}]...")

    if method == "ai":
        return idx, ocr_vlm(image_bytes)
    if method == "ocr":
        return idx, ocr_tesseract(image_bytes)
    if method == "auto_ai":
        vlm_text = ocr_vlm(image_bytes)
        return idx, vlm_text if vlm_text else text

    ocr_text = ocr_tesseract(image_bytes)
    if len(ocr_text) >= MIN_OCR_LEN:
        return idx, ocr_text
    vlm_text = ocr_vlm(image_bytes)
    return idx, vlm_text if vlm_text else text


def extract_pdf_method(filepath: Path, default_method: str = "auto") -> str:
    fc = get_file_config(filepath.name)
    default_method = fc.get("method", default_method)
    overrides = fc.get("overrides", [])

    doc = fitz.open(str(filepath))
    total = len(doc)

    page_methods = {i: default_method for i in range(total)}
    for ov in overrides:
        pages = parse_pages(ov.get("pages", ""), total)
        m = ov.get("method", default_method)
        for p in pages:
            page_methods[p] = m

    # 断点续传：加载已完成的结果
    results = load_progress(filepath)
    if results:
        print(f"  从进度文件恢复，已完成 {len(results)}/{total} 页")

    results_lock = threading.Lock()
    _unsaved_count = [0]

    def _collect_result(future, idx):
        try:
            _, text = future.result()
            with results_lock:
                results[idx] = text
                _unsaved_count[0] += 1
                if _unsaved_count[0] >= SAVE_INTERVAL:
                    save_progress(filepath, results)
                    _unsaved_count[0] = 0
        except Exception as e:
            print(f"  [页 {idx + 1} 失败] {e}")

    ocr_queue = queue.Queue(maxsize=MAX_WORKERS * 2)
    done_event = threading.Event()
    _executor_ref = [None]

    def ocr_consumer():
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            _executor_ref[0] = executor
            while not done_event.is_set() or not ocr_queue.empty():
                if _shutdown_flag.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                try:
                    task = ocr_queue.get(timeout=0.1)
                    if task is None:
                        break
                    future = executor.submit(process_ocr_page, task)
                    future.add_done_callback(lambda f, idx=task[0]: _collect_result(f, idx))
                except queue.Empty:
                    continue
            if _shutdown_flag.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=True)

    consumer_thread = threading.Thread(target=ocr_consumer, daemon=True)
    consumer_thread.start()

    try:
        print(f"  流水线处理 {total} 页 (workers={MAX_WORKERS}, dpi={DPI})...")
        for i in range(total):
            if _shutdown_flag.is_set():
                break
            if i in results:
                continue
            page = doc[i]
            method = page_methods[i]
            text = page.get_text().strip()

            needs_ocr = method in ("ai", "ocr") or (method in ("auto", "auto_ai") and len(text) < MIN_TEXT_LEN)

            if needs_ocr:
                pix = page.get_pixmap(dpi=DPI)
                img_bytes = pix.tobytes("png")
                while not _shutdown_flag.is_set():
                    try:
                        ocr_queue.put((i, total, method, text, img_bytes), timeout=0.5)
                        break
                    except queue.Full:
                        continue
            else:
                with results_lock:
                    results[i] = text
    finally:
        done_event.set()
        try:
            while not ocr_queue.empty():
                ocr_queue.get_nowait()
        except queue.Empty:
            pass
        ocr_queue.put(None)
        if _shutdown_flag.is_set() and _executor_ref[0]:
            _executor_ref[0].shutdown(wait=False, cancel_futures=True)
        consumer_thread.join(timeout=3)
        doc.close()
        save_progress(filepath, results)

    if _shutdown_flag.is_set():
        print(f"  已保存进度 ({len(results)}/{total} 页)")
        return ""

    # 全部完成，删除进度文件
    prog = _progress_path(filepath)
    if prog.exists():
        prog.unlink()

    return "\n\n---\n\n".join(results.get(i, "") for i in sorted(results))


def process_file(filepath: Path, method: str | None = None):
    suffix = filepath.suffix.lower()
    print(f"正在处理: {filepath.name}")

    fc = get_file_config(filepath.name)
    final_method = method or fc.get("method", "auto")

    if suffix == ".epub":
        content = extract_epub(filepath)
    elif suffix == ".pdf":
        content = extract_pdf_method(filepath, final_method)
        if _shutdown_flag.is_set():
            return
    else:
        print(f"不支持的文件格式: {suffix}")
        return
    out_dir = get_output_dir()
    out = out_dir / (filepath.stem + ".md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"已保存: {out}")


def _resolve_path(name: str, input_dir: Path) -> Path:
    f = Path(name)
    if not f.is_absolute() and not f.exists():
        f = input_dir / f
    return f


def main():
    parser = argparse.ArgumentParser(description="从 EPUB/PDF 提取文本为 Markdown")
    parser.add_argument("files", nargs="*", help="要处理的文件路径")
    parser.add_argument("-f", "--file", type=str, nargs="+", help="指定文件路径（支持多个）")
    parser.add_argument("-m", "--method", type=str, choices=["auto", "auto_ai", "text", "ocr", "ai", "ask"], help="指定处理方法")
    parser.add_argument("-w", "--workers", type=int, default=None, help="并行线程数")
    parser.add_argument("--w-full", action="store_true", help="使用最高线程数")
    parser.add_argument("--dpi", type=int, default=None, help="PDF 渲染 DPI（默认200）")
    parser.add_argument("-c", "--config", type=str, default="extract_config.toml", help="配置文件路径")
    args = parser.parse_args()

    cfg = load_config(args.config)
    init_globals(cfg, args)

    input_dir = get_input_dir()

    if args.file:
        targets = [_resolve_path(n, input_dir) for n in args.file]
    elif args.files:
        targets = [_resolve_path(n, input_dir) for n in args.files]
    else:
        targets = list(input_dir.glob("*.epub")) + list(input_dir.glob("*.pdf"))

    if not targets:
        print(f"在 {input_dir} 中未找到 epub/pdf 文件")
        sys.exit(1)

    for f in targets:
        if _shutdown_flag.is_set():
            break
        if not f.exists():
            print(f"文件不存在: {f}")
            continue
        process_file(f, args.method)


if __name__ == "__main__":
    main()
