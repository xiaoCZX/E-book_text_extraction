"""兼容入口：保持 python extract_text.py 可用。"""

from extract_text.__main__ import main

if __name__ == "__main__":
    import sys
    try:
        main()
    except Exception as e:
        print(f"\n程序异常退出: {e}", file=sys.stderr)
        sys.exit(1)
