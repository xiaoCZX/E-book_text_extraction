"""文件提取：EPUB 和 PDF 处理。"""

import queue
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import ebooklib
from ebooklib import epub
import fitz
import markdownify

from .config import cfg
from .clean import smart_clean
from .ocr import process_ocr_page
from .progress import load_progress, save_progress, delete_progress
from .utils import shutdown_flag, parse_pages


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


def extract_pdf_method(filepath: Path, default_method: str = "auto") -> str:
    fc = cfg.get_file_config(filepath.name)
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

    results = load_progress(filepath)
    if results:
        print(f"  从进度文件恢复，已完成 {len(results)}/{total} 页")

    results_lock = threading.Lock()
    _unsaved_count = [0]

    def _collect_result(future, idx):
        try:
            _, text = future.result()
            if not text and not shutdown_flag.is_set():
                print(f"  [页 {idx + 1} 结果为空，将在下次运行时重试]")
                return
            with results_lock:
                results[idx] = text
                _unsaved_count[0] += 1
                if _unsaved_count[0] >= cfg.save_interval:
                    save_progress(filepath, results)
                    _unsaved_count[0] = 0
        except Exception as e:
            print(f"  [页 {idx + 1} 失败] {e}")

    ocr_queue = queue.Queue(maxsize=cfg.max_workers)
    done_event = threading.Event()
    _executor_ref = [None]

    def ocr_consumer():
        with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
            _executor_ref[0] = executor
            while not done_event.is_set() or not ocr_queue.empty():
                if shutdown_flag.is_set():
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
            if shutdown_flag.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=True)

    consumer_thread = threading.Thread(target=ocr_consumer, daemon=True)
    consumer_thread.start()

    try:
        print(f"  流水线处理 {total} 页 (workers={cfg.max_workers}, dpi={cfg.dpi})...")
        for i in range(total):
            if shutdown_flag.is_set():
                break
            if i in results:
                continue
            page = doc[i]
            method = page_methods[i]
            text = page.get_text().strip()

            needs_ocr = method in ("ai", "ocr") or (method in ("auto", "auto_ai") and len(text) < cfg.min_text_len)

            if needs_ocr:
                pix = page.get_pixmap(dpi=cfg.dpi)
                img_bytes = pix.tobytes("png")
                while not shutdown_flag.is_set():
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
        if shutdown_flag.is_set() and _executor_ref[0]:
            _executor_ref[0].shutdown(wait=False, cancel_futures=True)
        consumer_thread.join(timeout=3)
        doc.close()
        save_progress(filepath, results)

    if shutdown_flag.is_set():
        print(f"  已保存进度 ({len(results)}/{total} 页)")
        return ""

    delete_progress(filepath)

    if cfg.enable_clean:
        print(f"  正在使用文本模型清洗 ({cfg.clean_model})...")
        sorted_keys = sorted(results)
        for pos, idx in enumerate(sorted_keys):
            if shutdown_flag.is_set():
                break
            prev_text = results.get(sorted_keys[pos - 1], "") if pos > 0 else ""
            next_text = results.get(sorted_keys[pos + 1], "") if pos < len(sorted_keys) - 1 else ""
            results[idx] = smart_clean(results[idx], prev_text, next_text)
            print(f"    清洗 {idx + 1}/{total} 完成")

    return "\n\n---\n\n".join(results.get(i, "") for i in sorted(results))


def process_file(filepath: Path, method: str | None = None):
    suffix = filepath.suffix.lower()
    print(f"正在处理: {filepath.name}")

    fc = cfg.get_file_config(filepath.name)
    final_method = method or fc.get("method", "auto")

    if suffix == ".epub":
        content = extract_epub(filepath)
    elif suffix == ".pdf":
        content = extract_pdf_method(filepath, final_method)
        if shutdown_flag.is_set():
            return
    else:
        print(f"不支持的文件格式: {suffix}")
        return

    out = cfg.output_dir / (filepath.stem + ".md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"已保存: {out}")
