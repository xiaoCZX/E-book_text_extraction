"""命令行入口：python -m extract_text"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .config import cfg, load_config, init_globals
from .extractors import process_file
from .utils import shutdown_flag, resolve_path

log = logging.getLogger(__name__)

_file_handler = None


def setup_logging():
    """配置日志：控制台 INFO。"""
    root = logging.getLogger("extract_text")
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)


def _set_file_log(filepath: Path):
    """为每个文件创建独立日志：log/时间_文件名.log"""
    global _file_handler
    root = logging.getLogger("extract_text")
    if _file_handler:
        root.removeHandler(_file_handler)
        _file_handler.close()
    log_dir = Path("log")
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{ts}_{filepath.stem}.log"
    _file_handler = logging.FileHandler(log_file, encoding="utf-8")
    _file_handler.setLevel(logging.DEBUG)
    _file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root.addHandler(_file_handler)


def main():
    parser = argparse.ArgumentParser(description="从 EPUB/PDF 提取文本为 Markdown")
    parser.add_argument("files", nargs="*", help="要处理的文件路径")
    parser.add_argument("-f", "--file", type=str, nargs="+", help="指定文件路径（支持多个）")
    parser.add_argument("-m", "--method", type=str, choices=["auto", "auto_ai", "text", "ocr", "ai", "ask"], help="指定处理方法")
    parser.add_argument("-w", "--workers", type=int, default=None, help="并行线程数")
    parser.add_argument("--w-full", action="store_true", help="使用最高线程数")
    parser.add_argument("--dpi", type=int, default=None, help="PDF 渲染 DPI（默认200）")
    parser.add_argument("--clean", action="store_true", help="使用文本模型清洗 VLM 输出")
    parser.add_argument("--split", action="store_true", help="每页单独保存为一个 .md 文件")
    parser.add_argument("-c", "--config", type=str, default="extract_config.toml", help="配置文件路径")
    args = parser.parse_args()

    raw_cfg = load_config(args.config)
    init_globals(raw_cfg, args)

    setup_logging()
    log.info("启动 method=%s workers=%d dpi=%d clean=%s split=%s",
             args.method or "auto", cfg.max_workers, cfg.dpi, cfg.enable_clean, args.split)

    input_dir = cfg.input_dir

    if args.file:
        targets = [resolve_path(n, input_dir) for n in args.file]
    elif args.files:
        targets = [resolve_path(n, input_dir) for n in args.files]
    else:
        targets = list(input_dir.glob("*.epub")) + list(input_dir.glob("*.pdf"))

    if not targets:
        log.error("在 %s 中未找到 epub/pdf 文件", input_dir)
        sys.exit(1)

    for f in targets:
        if shutdown_flag.is_set():
            break
        if not f.exists():
            log.warning("文件不存在: %s", f)
            continue
        _set_file_log(f)
        process_file(f, args.method, split=args.split)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("程序异常退出: %s", e)
        sys.exit(1)
