"""文件提取：EPUB 和 PDF 处理。"""

import logging
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

log = logging.getLogger(__name__)


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

    clean_queue = queue.Queue()
    ocr_all_done = threading.Event()
    clean_workers = min(cfg.max_workers, 8)

    def _clean_worker():
        while True:
            try:
                item = clean_queue.get(timeout=0.2)
            except queue.Empty:
                if ocr_all_done.is_set() and clean_queue.empty():
                    return
                continue
            if item is None:
                return
            idx, text = item
            if shutdown_flag.is_set():
                continue
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
                log.warning("页 %d/%d 无法修复，标记重新OCR", idx + 1, total)
                with retry_lock:
                    retry_pages.add(idx)
                with results_lock:
                    results.pop(idx, None)
            elif cleaned != TEXT_OK:
                with results_lock:
                    results[idx] = cleaned
                log.info("页 %d/%d 清洗已修正", idx + 1, total)
            else:
                log.info("页 %d/%d 清洗质量良好", idx + 1, total)

    clean_threads = []
    if do_clean:
        for _ in range(clean_workers):
            t = threading.Thread(target=_clean_worker, daemon=True)
            t.start()
            clean_threads.append(t)

    _pending = [len(tasks)]
    _pending_lock = threading.Lock()

    def _on_ocr_done(future, idx):
        try:
            _, text = future.result()
            with _pending_lock:
                _pending[0] -= 1
                remaining = _pending[0]
            if not text and not shutdown_flag.is_set():
                log.warning("页 %d OCR 结果为空 (剩余%d)", idx + 1, remaining)
                return
            need_save = False
            with results_lock:
                results[idx] = text
                _unsaved[0] += 1
                if _unsaved[0] >= cfg.save_interval:
                    _unsaved[0] = 0
                    need_save = True
            if need_save:
                with results_lock:
                    snapshot = dict(results)
                save_progress(filepath, snapshot)
            log.info("页 %d/%d OCR 完成 (剩余%d)", idx + 1, total, remaining)
            if do_clean and text:
                clean_queue.put((idx, text))
        except Exception as e:
            with _pending_lock:
                _pending[0] -= 1
            log.error("页 %d OCR 回调失败: %s", idx + 1, e)

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
        log.debug("ocr_done_event 已设置，等待放入 sentinel")
        while True:
            try:
                ocr_queue.put(None, timeout=0.5)
                break
            except queue.Full:
                if shutdown_flag.is_set():
                    break
        if shutdown_flag.is_set():
            try:
                while not ocr_queue.empty():
                    ocr_queue.get_nowait()
            except queue.Empty:
                pass
            if _executor_ref[0]:
                _executor_ref[0].shutdown(wait=False, cancel_futures=True)
        log.debug("等待 OCR consumer 线程结束...")
        while consumer_thread.is_alive():
            consumer_thread.join(timeout=5)
            if shutdown_flag.is_set():
                break
            if consumer_thread.is_alive():
                with _pending_lock:
                    r = _pending[0]
                log.info("等待最后 %d 个 OCR 请求完成...", r)
        log.debug("OCR consumer 已结束")
        if do_clean:
            ocr_all_done.set()
            log.debug("等待清洗线程结束，队列剩余 %d", clean_queue.qsize())
            for i, t in enumerate(clean_threads):
                while t.is_alive():
                    t.join(timeout=3)
                    if shutdown_flag.is_set():
                        break
                    if t.is_alive():
                        log.debug("清洗线程 %d 仍在运行，队列剩余 %d", i, clean_queue.qsize())
        save_progress(filepath, results)
        log.info("流水线结束，当前完成 %d 页", len(results))

    return retry_pages


def extract_pdf_method(filepath: Path, default_method: str = "auto") -> dict[int, str]:
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
        log.info("从进度文件恢复，已完成 %d/%d 页", len(results), total)

    results_lock = threading.Lock()

    first_tasks = []
    skipped_text = 0
    for i in range(total):
        if shutdown_flag.is_set():
            break
        if i in results:
            continue
        page = doc[i]
        method = page_methods[i]
        text = page.get_text().strip()

        # 检测页面是否包含大面积图片
        has_images = False
        if method in ("auto", "auto_ai"):
            images = page.get_images(full=True)
            if images:
                page_area = page.rect.width * page.rect.height
                img_area = 0
                for img in images:
                    xref = img[0]
                    try:
                        bbox = page.get_image_bbox(img)
                        img_area += bbox.width * bbox.height
                    except Exception:
                        pass
                if page_area > 0 and img_area / page_area > 0.3:
                    has_images = True
                    log.debug("页 %d 含大面积图片 (%.0f%%)", i + 1, img_area / page_area * 100)

        needs_ocr = method in ("ai", "ocr") or (
            method in ("auto", "auto_ai") and (len(text) < cfg.min_text_len or has_images)
        )
        if needs_ocr:
            pix = page.get_pixmap(dpi=cfg.dpi)
            img_bytes = pix.tobytes("png")
            first_tasks.append((i, total, method, text, img_bytes))
            log.debug("页 %d 需要OCR method=%s text_len=%d", i + 1, method, len(text))
        else:
            with results_lock:
                results[i] = text
            skipped_text += 1

    log.info("共 %d 页: %d 页需OCR, %d 页纯文本, %d 页已完成",
             total, len(first_tasks), skipped_text, len(results) - skipped_text)

    if first_tasks:
        log.info("流水线处理 workers=%d dpi=%d clean=%s", cfg.max_workers, cfg.dpi, cfg.enable_clean)
        retry_pages = _run_pipeline(first_tasks, results, results_lock, filepath, total, cfg.enable_clean)
    else:
        retry_pages = set()

    if shutdown_flag.is_set():
        doc.close()
        log.info("中断，已保存进度 %d/%d 页", len(results), total)
        return {}

    max_retry_rounds = 3
    for round_num in range(1, max_retry_rounds + 1):
        if not retry_pages or shutdown_flag.is_set():
            break
        log.info("重试第 %d/%d 轮，共 %d 页: %s", round_num, max_retry_rounds, len(retry_pages), sorted(retry_pages))
        retry_tasks = []
        for i in sorted(retry_pages):
            pix = doc[i].get_pixmap(dpi=cfg.dpi)
            img_bytes = pix.tobytes("png")
            retry_tasks.append((i, total, "ai", "", img_bytes))
        retry_pages = _run_pipeline(retry_tasks, results, results_lock, filepath, total, cfg.enable_clean)

    if retry_pages:
        log.warning("%d 页在 %d 轮重试后仍失败: %s", len(retry_pages), max_retry_rounds, sorted(retry_pages))
        for idx in retry_pages:
            results.setdefault(idx, "")

    doc.close()

    if shutdown_flag.is_set():
        log.info("中断，已保存进度 %d/%d 页", len(results), total)
        return {}

    delete_progress(filepath)
    return results


def process_file(filepath: Path, method: str | None = None, split: bool = False):
    suffix = filepath.suffix.lower()
    log.info("开始处理: %s method=%s split=%s", filepath.name, method or "auto", split)

    fc = cfg.get_file_config(filepath.name)
    final_method = method or fc.get("method", "auto")

    if suffix == ".epub":
        content = extract_epub(filepath)
        if split:
            out_dir = cfg.output_dir / filepath.stem
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "001.md").write_text(content, encoding="utf-8")
        else:
            out = cfg.output_dir / (filepath.stem + ".md")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
        log.info("已保存: %s", cfg.output_dir / filepath.stem)
        return

    if suffix == ".pdf":
        results = extract_pdf_method(filepath, final_method)
        if shutdown_flag.is_set():
            return
        if isinstance(results, dict):
            if split:
                out_dir = cfg.output_dir / filepath.stem
                out_dir.mkdir(parents=True, exist_ok=True)
                total = max(results.keys()) + 1 if results else 0
                width = len(str(total))
                count = 0
                for i in sorted(results):
                    text = results[i].strip()
                    if text:
                        page_file = out_dir / f"{str(i + 1).zfill(width)}.md"
                        page_file.write_text(text, encoding="utf-8")
                        count += 1
                log.info("已保存: %s (%d 页)", out_dir, count)
            else:
                parts = [results[i] for i in sorted(results) if results.get(i, "").strip()]
                content = "\n\n---\n\n".join(parts)
                out = cfg.output_dir / (filepath.stem + ".md")
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_text(content, encoding="utf-8")
                log.info("已保存: %s", out)
        return

    log.warning("不支持的文件格式: %s", suffix)
