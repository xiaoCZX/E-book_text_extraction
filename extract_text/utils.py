"""工具函数和全局信号管理。"""

import os
import signal
import threading
from pathlib import Path

shutdown_flag = threading.Event()

_ctrl_c_count = 0


def _signal_handler(sig, frame):
    global _ctrl_c_count
    _ctrl_c_count += 1
    shutdown_flag.set()
    if _ctrl_c_count >= 2:
        print("\n强制退出!")
        os._exit(1)
    print("\n收到中断信号，正在优雅退出...（再按一次强制退出）")


signal.signal(signal.SIGINT, _signal_handler)


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


def resolve_path(name: str, input_dir: Path) -> Path:
    f = Path(name)
    if not f.is_absolute() and not f.exists():
        f = input_dir / f
    return f
