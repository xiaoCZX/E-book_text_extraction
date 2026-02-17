"""文本清洗：Markdown 清理、垃圾检测、智能清洗。"""

import re

from openai import OpenAI

from .config import cfg
from .utils import shutdown_flag


def clean_markdown(text: str) -> str:
    text = re.sub(r'<\|begin_of_box\|>', '', text)
    text = re.sub(r'<\|end_of_box\|>', '', text)
    text = re.sub(r'```markdown\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = re.sub(r'^(以下|下面)是?[^\n]*?(提取|识别|转换|输出|整理)[^\n]*?[:：]\s*', '', text)
    text = re.sub(r'^根据图片[^\n]*?[:：]\s*', '', text)
    text = re.sub(r'^图片中的[^\n]*?[:：]\s*', '', text)
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    return text.strip()


def is_garbage(text: str) -> bool:
    """检测模型幻觉/垃圾输出。"""
    if not text or len(text) < 10:
        return True
    for _ in re.finditer(r'(.{4,50})\1{4,}', text):
        return True
    cjk_en = len(re.findall(r'[\u4e00-\u9fff\u3000-\u303fa-zA-Z0-9，。！？、；：\u201c\u201d\u2018\u2019（）\s]', text))
    if len(text) > 50 and cjk_en / len(text) < 0.5:
        return True
    return False


def needs_clean_local(text: str) -> bool:
    """本地规则检测是否需要清洗。"""
    if not text:
        return False
    if '\\n' in text or '\\t' in text:
        return True
    paragraphs = [p.strip() for p in text.split('\n\n') if len(p.strip()) > 20]
    if paragraphs and len(paragraphs) != len(set(paragraphs)):
        return True
    short_run = 0
    for line in text.split('\n'):
        if 0 < len(line.strip()) < 5:
            short_run += 1
            if short_run >= 10:
                return True
        else:
            short_run = 0
    return False


def should_clean(text: str) -> bool:
    """用 tool_model 快速判断是否需要清洗。"""
    model = cfg.next_tool_model()
    if not model or shutdown_flag.is_set():
        return False
    try:
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base, timeout=30)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": f"以下文本是否包含OCR错误、乱码或需要修正的问题？只回答Y或N。\n\n{text[:500]}"}],
            max_tokens=1,
        )
        return resp.choices[0].message.content.strip().upper().startswith("Y")
    except Exception:
        return True


def text_clean(text: str, prev_text: str = "", next_text: str = "") -> str:
    """用文本模型清洗 OCR 结果。"""
    if not cfg.clean_model or not text or shutdown_flag.is_set():
        return text
    context = ""
    if prev_text:
        context += f"[上一页末尾]\n{prev_text[-200:]}\n\n"
    context += f"[当前页内容]\n{text}\n"
    if next_text:
        context += f"\n[下一页开头]\n{next_text[:200]}"
    try:
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base, timeout=60)
        resp = client.chat.completions.create(
            model=cfg.clean_model,
            messages=[{
                "role": "user",
                "content": f"以下是OCR识别的书籍页面文本，可能包含识别错误、乱码、重复内容或模型幻觉。请清洗并修正文本，只输出修正后的当前页内容，保留Markdown格式。如果内容本身就是正常的，原样输出即可。\n\n{context}",
            }],
            max_tokens=4096,
        )
        cleaned = resp.choices[0].message.content.strip()
        return clean_markdown(cleaned) if cleaned else text
    except Exception as e:
        print(f"  [文本清洗失败] {e}")
        return text


def smart_clean(text: str, prev_text: str = "", next_text: str = "") -> str:
    """三级过滤智能清洗。"""
    if not cfg.clean_model or not text or shutdown_flag.is_set():
        return text
    if needs_clean_local(text):
        return text_clean(text, prev_text, next_text)
    if should_clean(text):
        return text_clean(text, prev_text, next_text)
    return text
