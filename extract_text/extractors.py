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
from .clean import smart_clean, TEXT_ERROR, TEXT_OK
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


def _run_pipeline(tasks: list[tuple], results: dict, results_lock: threading.Lock,
                  filepath: Path, total: int, do_clean: bool) -> set[int]:
    """OCR + 清洗并行流水线。返回清洗失败(TEXT_ERROR)的页码集合。"""
    if not tasks or shutdown_flag.is_set():
        return set()

    retry_pages: set[int] = set()
    retry_lock = threading.Lock()
    _unsaved = [0]

    # 清洗队列：OCR 完成一页就丢进来
    clean_queue = queue.Queue()
    clean_done = threading.Event()

    # --- 清洗线程 ---
    clean_workers = min(cfg.max_workers, 8)

    def _clean_worker():
        while True:
            try:
                item = clean_queue.get(timeout=0.2)
            except queue.Empty:
                if clean_done.is_set() and clean_queue.empty():
                    return
                continue
            if item is None:
                return
            idx, text = item
            if shutdown_flag.is_set():
                continue
            # 获取上下文（尽力而为，可能邻页还没完成）
            with results_lock:
                sorted_keys = sorted(results)
            pos = -1
            for p, k in enumerate(sorted_keys):
                if k == idx:
                    pos = p
                    break
            prev_text = results.get(sorted_keys[pos - 1], "") if pos > 0 else ""
            next_text = results.get(sorted_keys[pos + 1], "") if pos >= 0 and pos < len(sorted_keys) - 1 else ""

            cleaned = smart_clean(text, prev_text, next_text)
            if cleaned == TEXT_ERROR:
                print(f"    页 {idx + 1}/{total} 无法修复，标记重新OCR")
                with retry_lock:
                    retry_pages.add(idx)
                with results_lock:
                    results.pop(idx, None)
            elif cleaned != TEXT_OK:
                with results_lock:
                    results[idx] = cleaned
                print(f"    清洗 {idx + 1}/{total} 已修正")
            else:
                print(f"    清洗 {idx + 1}/{total} 质量良好")

    clean_threads = []
    if do_clean:
        for _ in range(clean_workers):
            t = threading.Thread(target=_clean_worker, daemon=True)
            t.start()
            clean_threads.append(t)

    # --- OCR 回调：结果写入 results，然后投入清洗队列 ---
    def _on_ocr_done(future, idx):
        try:
            _, text = future.result()
            if not text and not shutdown_flag.is_set():
                return
            with results_lock:
                results[idx] = text
                _unsaved[0] += 1
                if _unsaved[0] >= cfg.save_interval:
                    save_progress(filepath, results)
                    _unsaved[0] = 0
            if do_clean and text:
                clean_queue.put((idx, text))
        except Exception as e:
            print(f"  [页 {idx + 1} 失败] {e}")

    # --- OCR 流水线 ---
    ocr_queue = queue.Queue(maxsize=cfg.max_workers)
    ocr_done_event = threading.Event()
    _executor_ref = [None]

    def ocr_consumer():
        with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
            _executor_ref[0] = executor
            while not ocr_done_event.is_set() or not ocr_queue.empty():
                if shutdown_flag.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    return
                try:
                    task = ocr_queue.get(timeout=0.1)
                    if task is None:
                        break
                    future = executor.submit(process_ocr_page, task)
                    future.add_done_callback(lambda f, idx=task[0]: _on_ocr_done(f, idx))
                except queue.Empty:
                    continue
            if shutdown_flag.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=True)

    consumer_thread = threading.Thread(target=ocr_consumer, daemon=True)
    consumer_thread.start()

    try:
        for task in tasks:
            if shutdown_flag.is_set():
                break
            while not shutdown_flag.is_set():
                try:
                    ocr_queue.put(task, timeout=0.5)
                    break
                except queue.Full:
                    continue
    finally:
        ocr_done_event.set()
        try:
            while not ocr_queue.empty():
                ocr_queue.get_nowait()
        except queue.Empty:
            pass
        ocr_queue.put(None)
        if shutdown_flag.is_set() and _executor_ref[0]:
            _executor_ref[0].shutdown(wait=False, cancel_futures=True)
        # 循环等待 OCR 线程真正结束
        while consumer_thread.is_alive():
            consumer_thread.join(timeout=1)
            if shutdown_flag.is_set():
                break

        # 等清洗线程处理完队列中所有任务
        if do_clean:
            clean_done.set()
            for t in clean_threads:
                while t.is_alive():
                    t.join(timeout=1)
                    if shutdown_flag.is_set():
                        break

        save_progress(filepath, results)

    return retry_pages


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

    # === 构建首轮任务 ===
    first_tasks = []
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
            first_tasks.append((i, total, method, text, img_bytes))
        else:
            with results_lock:
                results[i] = text

    if first_tasks:
        print(f"  流水线处理 {total} 页 (workers={cfg.max_workers}, dpi={cfg.dpi})...")
        retry_pages = _run_pipeline(first_tasks, results, results_lock, filepath, total, cfg.enable_clean)
    else:
        retry_pages = set()

    if shutdown_flag.is_set():
        doc.close()
        print(f"  已保存进度 ({len(results)}/{total} 页)")
        return ""

    # === 重试阶段 ===
    max_retry_rounds = 3
    for round_num in range(1, max_retry_rounds + 1):
        if not retry_pages or shutdown_flag.is_set():
            break
        print(f"  重试第 {round_num}/{max_retry_rounds} 轮，共 {len(retry_pages)} 页...")
        retry_tasks = []
        for i in sorted(retry_pages):
            pix = doc[i].get_pixmap(dpi=cfg.dpi)
            img_bytes = pix.tobytes("png")
            retry_tasks.append((i, total, "ai", "", img_bytes))
        retry_pages = _run_pipeline(retry_tasks, results, results_lock, filepath, total, cfg.enable_clean)

    if retry_pages:
        print(f"  警告：{len(retry_pages)} 页在 {max_retry_rounds} 轮重试后仍失败: {sorted(retry_pages)}")
        for idx in retry_pages:
            results.setdefault(idx, "")

    doc.close()

    if shutdown_flag.is_set():
        print(f"  已保存进度 ({len(results)}/{total} 页)")
        return ""

    delete_progress(filepath)
    # 跳过空页面，避免大量空 --- 分隔符
    parts = [results[i] for i in range(total) if results.get(i, "").strip()]
    return "\n\n---\n\n".join(parts)


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
