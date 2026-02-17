"""OCR 引擎：Tesseract 和 VLM API。"""

import base64
import io
import time

from openai import OpenAI
from PIL import Image
import pytesseract

from .config import cfg
from .clean import clean_markdown, is_garbage
from .utils import shutdown_flag


def ocr_tesseract(image_bytes: bytes) -> str:
    try:
        img = Image.open(io.BytesIO(image_bytes))
        return pytesseract.image_to_string(img, lang=cfg.tess_lang).strip()
    except Exception as e:
        print(f"  [Tesseract 失败] {e}")
        return ""


def ocr_vlm(image_bytes: bytes) -> str:
    if shutdown_flag.is_set():
        return ""
    model = cfg.next_model()
    try:
        b64 = base64.b64encode(image_bytes).decode()
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base, timeout=120)
        for attempt in range(3):
            if shutdown_flag.is_set():
                return ""
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                            {"type": "text", "text": "直接输出这张图片中的所有文字内容。保留标题、段落、列表等原文格式，使用Markdown语法。禁止添加任何解释、前言、总结或代码块标记。"},
                        ],
                    }],
                    max_tokens=4096,
                )
                content = resp.choices[0].message.content.strip()
                cleaned = clean_markdown(content)
                if is_garbage(cleaned):
                    if attempt < 2 and not shutdown_flag.is_set():
                        print(f"  [VLM 质量差，重试 {attempt + 1}/3 model={model}]")
                        model = cfg.next_model()
                        continue
                    return ""
                return cleaned
            except Exception as e:
                if attempt < 2 and not shutdown_flag.is_set():
                    err_str = str(e).lower()
                    if not any(k in err_str for k in ("429", "rate", "connection", "timeout", "500", "502", "503", "504")):
                        raise
                    wait = (attempt + 1) * 3
                    print(f"  [VLM 重试 {attempt + 1}/3 model={model}] {e} (等待{wait}s)")
                    time.sleep(wait)
                    model = cfg.next_model()
                    continue
                raise
    except Exception as e:
        print(f"  [VLM API 失败 model={model}] {e}")
        return ""


def process_ocr_page(args: tuple) -> tuple[int, str]:
    idx, total, method, text, image_bytes = args
    if shutdown_flag.is_set():
        return idx, text
    print(f"  OCR 第 {idx + 1}/{total} 页 [method={method}]...")

    if method == "ai":
        return idx, ocr_vlm(image_bytes)
    if method == "ocr":
        return idx, ocr_tesseract(image_bytes)
    if method == "auto_ai":
        vlm_text = ocr_vlm(image_bytes)
        return idx, vlm_text if vlm_text else text

    ocr_text = ocr_tesseract(image_bytes)
    if len(ocr_text) >= cfg.min_ocr_len:
        return idx, ocr_text
    vlm_text = ocr_vlm(image_bytes)
    return idx, vlm_text if vlm_text else text
