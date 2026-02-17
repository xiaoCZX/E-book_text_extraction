"""OCR 引擎：Tesseract 和 VLM API。"""

import base64
import io
import logging
import time

from openai import OpenAI
from PIL import Image
import pytesseract

from .config import cfg
from .clean import clean_markdown, is_garbage
from .utils import shutdown_flag

log = logging.getLogger(__name__)


def ocr_tesseract(image_bytes: bytes) -> str:
    try:
        img = Image.open(io.BytesIO(image_bytes))
        return pytesseract.image_to_string(img, lang=cfg.tess_lang).strip()
    except Exception as e:
        log.warning("Tesseract 失败: %s", e)
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
                log.debug("VLM 请求 model=%s attempt=%d", model, attempt + 1)
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
                            {"type": "text", "text": "你是OCR文字提取工具。请逐字提取这张图片中的所有文字内容。\n\n严格要求：\n1. 按原文阅读顺序横向输出，每个自然段落为一段\n2. 保留标题、段落、列表等格式，使用Markdown语法\n3. 只输出图片中实际存在的文字，禁止生成任何图片中不存在的内容\n4. 禁止逐字竖排输出，禁止每个字单独一行\n5. 禁止添加解释、前言、总结、评论、格式说明、排版建议或代码块标记\n6. 禁止输出HTML标签\n7. 如果图片中没有文字，输出空内容"},
                        ],
                    }],
                    max_tokens=4096,
                )
                content = resp.choices[0].message.content.strip()
                cleaned = clean_markdown(content)
                if is_garbage(cleaned):
                    if attempt < 2 and not shutdown_flag.is_set():
                        log.warning("VLM 质量差 model=%s attempt=%d/%d，切换模型重试", model, attempt + 1, 3)
                        model = cfg.next_model()
                        continue
                    log.warning("VLM 3次均为垃圾输出 model=%s", model)
                    return ""
                log.info("VLM 成功 model=%s len=%d", model, len(cleaned))
                return cleaned
            except Exception as e:
                if attempt < 2 and not shutdown_flag.is_set():
                    err_str = str(e).lower()
                    if not any(k in err_str for k in ("429", "rate", "connection", "timeout", "500", "502", "503", "504")):
                        raise
                    wait = (attempt + 1) * 3
                    log.warning("VLM 重试 %d/3 model=%s err=%s 等待%ds", attempt + 1, model, e, wait)
                    time.sleep(wait)
                    model = cfg.next_model()
                    continue
                raise
    except Exception as e:
        log.error("VLM API 失败 model=%s: %s", model, e)
        return ""


def process_ocr_page(args: tuple) -> tuple[int, str]:
    idx, total, method, text, image_bytes = args
    if shutdown_flag.is_set():
        return idx, text
    log.info("OCR 第 %d/%d 页 method=%s", idx + 1, total, method)

    if method == "ai":
        return idx, ocr_vlm(image_bytes)
    if method == "ocr":
        return idx, ocr_tesseract(image_bytes)
    if method == "auto_ai":
        vlm_text = ocr_vlm(image_bytes)
        if vlm_text:
            return idx, vlm_text
        log.debug("页 %d VLM 为空，回退到原文本 len=%d", idx + 1, len(text))
        return idx, text

    # auto 模式
    ocr_text = ocr_tesseract(image_bytes)
    if len(ocr_text) >= cfg.min_ocr_len:
        log.debug("页 %d Tesseract 足够 len=%d", idx + 1, len(ocr_text))
        return idx, ocr_text
    vlm_text = ocr_vlm(image_bytes)
    return idx, vlm_text if vlm_text else text
