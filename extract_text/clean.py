"""文本清洗：Markdown 清理、垃圾检测、智能清洗。"""

import re

from openai import OpenAI

from .config import cfg
from .utils import shutdown_flag

# smart_clean 返回此常量表示文本无法修复，需要打回重新 OCR
TEXT_ERROR = "text_error"
# 清洗模型返回此值表示原文质量良好无需修改
TEXT_OK = "TRUE"


def clean_markdown(text: str) -> str:
    text = re.sub(r'<\|begin_of_box\|>', '', text)
    text = re.sub(r'<\|end_of_box\|>', '', text)
    text = re.sub(r'```markdown\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    # 清除 HTML 标签残留
    text = re.sub(r'</?(?:u|b|i|em|strong|h[1-6]|span|div|p|br|hr)\s*/?>', '', text, flags=re.IGNORECASE)
    # 过滤模型前言/总结
    text = re.sub(r'^(以下|下面)是?[^\n]*?(提取|识别|转换|输出|整理)[^\n]*?[:：]\s*', '', text)
    text = re.sub(r'^根据图片[^\n]*?[:：]\s*', '', text)
    text = re.sub(r'^图片中的[^\n]*?[:：]\s*', '', text)
    # 过滤模型废话整行（各种自创指令、格式说明、与图片相关的废话）
    text = re.sub(
        r'^[^\n]*(?:仅对图片|与图片相关|不应包含在本文|非正文文本'
        r'|使用Markdown格式|提交Word文档|有问题请发消息'
        r'|字体字体大小|封面中需保持|左对齐|加粗标记|使用斜体'
        r'|另外提供一个建议)[^\n]*$',
        '', text, flags=re.MULTILINE
    )
    # 过滤模型自创的编号格式指令（如 "6. 只保留标识字符，不配"）
    text = re.sub(
        r'^\d+\.\s*(?:只保留|所有文字均|空的Markdown|同时提交)[^\n]*$',
        '', text, flags=re.MULTILINE
    )
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
    # 竖排单字断行检测：大量连续单字行
    lines = text.split('\n')
    if len(lines) > 10:
        single_char_lines = sum(1 for l in lines if len(l.strip()) == 1)
        if single_char_lines / len(lines) > 0.5:
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
    """用 tool_model 快速判断文本质量。始终调用，不省略。"""
    model = cfg.next_tool_model()
    if not model or shutdown_flag.is_set():
        return False
    try:
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base, timeout=30)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": (
                "你是OCR文本质量审核员。请判断以下文本是否存在需要修正的问题。\n"
                "判断标准：\n"
                "- 存在明显的OCR识别错误（如错别字、形近字混淆）\n"
                "- 存在乱码、不可读字符\n"
                "- 存在大段重复内容\n"
                "- 存在模型幻觉（与书籍内容无关的生成内容）\n"
                "- 格式严重混乱（如标题层级错误、列表格式损坏）\n\n"
                "如果文本质量良好、内容通顺可读，回答N。\n"
                "如果存在上述任何问题，回答Y。\n"
                "只回答一个字母Y或N，不要解释。\n\n"
                f"{text[:800]}"
            )}],
            max_tokens=1,
        )
        return resp.choices[0].message.content.strip().upper().startswith("Y")
    except Exception:
        return True


def text_clean(text: str, prev_text: str = "", next_text: str = "") -> str:
    """用文本模型清洗 OCR 结果。返回清洗后文本，或 TEXT_ERROR 表示无法修复。"""
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
                "content": (
                    "你是专业的OCR文本校对员。请对以下书籍页面的OCR识别结果进行校对和修正。\n\n"
                    "## 工作规则\n"
                    "1. 修正明显的OCR识别错误（形近字混淆、多余/缺失字符）\n"
                    "2. 修正乱码和不可读字符\n"
                    "3. 去除重复的段落或句子\n"
                    "4. 去除模型幻觉内容（与上下文明显无关的生成内容）\n"
                    "5. 保留原文的Markdown格式（标题、列表、粗体等）\n"
                    "6. 不要添加任何解释、前言、总结或代码块标记\n"
                    "7. 如果当前页内容完全是乱码、重复字句或无意义内容，无法修复，"
                    "则只输出 TEXT_ERROR\n"
                    "8. 如果内容质量良好、无需任何修改，只输出 TRUE\n"
                    "9. 只有确实需要修正时，才输出修正后的完整当前页内容\n\n"
                    "## 上下文参考（仅供理解语境，不要输出这些内容）\n\n"
                    f"{context}"
                ),
            }],
            max_tokens=4096,
        )
        cleaned = resp.choices[0].message.content.strip()
        if not cleaned:
            return text
        if cleaned == "TEXT_ERROR" or cleaned.startswith("TEXT_ERROR"):
            return TEXT_ERROR
        if cleaned == "TRUE" or cleaned == "True":
            return TEXT_OK
        return clean_markdown(cleaned)
    except Exception as e:
        print(f"  [文本清洗失败] {e}")
        return text


def smart_clean(text: str, prev_text: str = "", next_text: str = "") -> str:
    """三级过滤智能清洗。返回：清洗后文本 / 原文本 / TEXT_ERROR。"""
    if not cfg.clean_model or not text or shutdown_flag.is_set():
        return text
    # 本地规则命中 → 直接送清洗
    if needs_clean_local(text):
        return text_clean(text, prev_text, next_text)
    # tool_model 判断 → 需要清洗则送清洗
    if should_clean(text):
        return text_clean(text, prev_text, next_text)
    # 质量良好，保留原文
    return text
