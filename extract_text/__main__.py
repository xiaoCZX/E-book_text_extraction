"""命令行入口：python -m extract_text"""

import argparse
import sys

from .config import cfg, load_config, init_globals
from .extractors import process_file
from .utils import shutdown_flag, resolve_path


def main():
    parser = argparse.ArgumentParser(description="从 EPUB/PDF 提取文本为 Markdown")
    parser.add_argument("files", nargs="*", help="要处理的文件路径")
    parser.add_argument("-f", "--file", type=str, nargs="+", help="指定文件路径（支持多个）")
    parser.add_argument("-m", "--method", type=str, choices=["auto", "auto_ai", "text", "ocr", "ai", "ask"], help="指定处理方法")
    parser.add_argument("-w", "--workers", type=int, default=None, help="并行线程数")
    parser.add_argument("--w-full", action="store_true", help="使用最高线程数")
    parser.add_argument("--dpi", type=int, default=None, help="PDF 渲染 DPI（默认200）")
    parser.add_argument("--clean", action="store_true", help="使用文本模型清洗 VLM 输出")
    parser.add_argument("-c", "--config", type=str, default="extract_config.toml", help="配置文件路径")
    args = parser.parse_args()

    raw_cfg = load_config(args.config)
    init_globals(raw_cfg, args)

    input_dir = cfg.input_dir

    if args.file:
        targets = [resolve_path(n, input_dir) for n in args.file]
    elif args.files:
        targets = [resolve_path(n, input_dir) for n in args.files]
    else:
        targets = list(input_dir.glob("*.epub")) + list(input_dir.glob("*.pdf"))

    if not targets:
        print(f"在 {input_dir} 中未找到 epub/pdf 文件")
        sys.exit(1)

    for f in targets:
        if shutdown_flag.is_set():
            break
        if not f.exists():
            print(f"文件不存在: {f}")
            continue
        process_file(f, args.method)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n程序异常退出: {e}", file=sys.stderr)
        sys.exit(1)
