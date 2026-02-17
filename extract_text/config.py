"""配置管理：加载、校验、全局状态。"""

import itertools
import os
import sys
import threading
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import pytesseract


@dataclass
class AppConfig:
    raw_cfg: dict = field(default_factory=dict)
    api_key: str = ""
    api_base: str = "https://api.siliconflow.cn/v1"
    models: list[str] = field(default_factory=list)
    clean_model: str = ""
    tool_models: list[str] = field(default_factory=list)
    enable_clean: bool = False
    tess_lang: str = "chi_sim+eng"
    min_text_len: int = 50
    min_ocr_len: int = 20
    max_workers: int = 4
    dpi: int = 200
    save_interval: int = 10
    file_dirs: dict = field(default_factory=dict)
    files_cfg: list = field(default_factory=list)

    _model_cycle: object = field(default=None, init=False, repr=False)
    _tool_cycle: object = field(default=None, init=False, repr=False)
    _model_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def next_model(self) -> str:
        with self._model_lock:
            return next(self._model_cycle)

    def next_tool_model(self) -> str:
        with self._model_lock:
            if self._tool_cycle:
                return next(self._tool_cycle)
            return self.clean_model

    @property
    def input_dir(self) -> Path:
        return Path(self.file_dirs.get("input_dir", "."))

    @property
    def output_dir(self) -> Path:
        return Path(self.file_dirs.get("output_dir", "."))

    def get_file_config(self, filename: str) -> dict:
        for fc in self.files_cfg:
            if fc.get("name") == filename:
                return fc
        return {}


cfg = AppConfig()


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
models = [
    "zai-org/GLM-4.6V", 
    "Qwen/Qwen3-VL-32B-Instruct",
    "Qwen/Qwen3-VL-8B-Instruct",
    "Qwen/Qwen3-VL-30B-A3B-Instruct",
    "Qwen/Qwen3-VL-235B-A22B-Instruct",
    "Qwen/Qwen3-Omni-30B-A3B-Instruct",
    "deepseek-ai/DeepSeek-OCR"
]
clean_model = ""
tool_models = []

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
        raw = tomllib.load(f)
    if not raw.get("api", {}).get("key", ""):
        print(f"错误: 请在 {config_file} 中填写 [api] key")
        sys.exit(1)
    return raw


def init_globals(raw_cfg: dict, args):
    """根据配置和命令行参数填充全局 cfg 单例。"""
    settings = raw_cfg.get("settings", {})
    api_cfg = raw_cfg.get("api", {})

    cfg.raw_cfg = raw_cfg
    cfg.file_dirs = raw_cfg.get("file_dirs", {})

    # files 配置
    files_list = raw_cfg.get("files", [])
    if not files_list:
        files_list = raw_cfg.get("files_config", {}).get("files", [])
    cfg.files_cfg = files_list

    # Tesseract
    pytesseract.pytesseract.tesseract_cmd = settings.get(
        "tesseract_cmd", r"D:\Program Files (x86)\Tesseract-OCR\tesseract.exe"
    )
    cfg.tess_lang = settings.get("tesseract_lang", "chi_sim+eng")
    cfg.min_text_len = settings.get("min_text_len", 50)
    cfg.min_ocr_len = settings.get("min_ocr_len", 20)
    cfg.save_interval = settings.get("save_interval", 10)

    # DPI: 命令行 > 配置文件 > 默认200
    cfg.dpi = args.dpi or settings.get("dpi", 200)

    # Workers
    if args.w_full:
        cfg.max_workers = min((os.cpu_count() or 4) * 4, 196)
    elif args.workers:
        cfg.max_workers = args.workers
    else:
        cfg.max_workers = settings.get("max_workers", 4)

    # API
    cfg.api_key = api_cfg.get("key", "")
    cfg.api_base = api_cfg.get("base_url", "https://api.siliconflow.cn/v1")

    models = api_cfg.get("models", ["zai-org/GLM-4.6V"])
    if not models or models == ["zai-org/GLM-4.6V"]:
        single = api_cfg.get("model", "")
        if single:
            models = [single]
    cfg.models = models
    cfg._model_cycle = itertools.cycle(models)

    # 清洗
    cfg.clean_model = api_cfg.get("clean_model", "")
    tool_models = api_cfg.get("tool_models", [])
    if not tool_models:
        # 兼容单个 tool_model 配置
        single_tool = api_cfg.get("tool_model", "")
        if single_tool:
            tool_models = [single_tool]
    cfg.tool_models = tool_models
    cfg._tool_cycle = itertools.cycle(tool_models) if tool_models else None
    cfg.enable_clean = getattr(args, "clean", False) and bool(cfg.clean_model)
