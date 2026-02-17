import base64
import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

# 配置参数
API_KEY = ""  # 这里写你们的哈基流动API
MODEL = "Qwen/Qwen3-VL-30B-A3B-Thinking"  # 咱个人认为性价比最高的多模态模型
PDF_FILE = ""  # 这里写你们的pdf文件路径

# 请求频率控制参数 根据模型卡里的显示的配额来设置
RPM = 20  # 每分钟请求数
TPM = 10000  # 每分钟token数

# 自动识别CPU逻辑核心数
CPU_CORES = multiprocessing.cpu_count()
print(f"✨ 检测到 {CPU_CORES} 个CPU逻辑核心")

# 计算线程数和请求间隔
MAX_THREADS = min(CPU_CORES, RPM // 2) # 线程数不超过CPU逻辑核心数
REQUEST_INTERVAL = 60 / RPM if RPM > 0 else 1

# 重试次数
MAX_RETRIES = 9999999999999  #2147483647其实够了（）


def encode_image_to_base64(image_path):
    """将图片编码为base64字符串"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


def ocr_image_with_ai_model(base64_image, page_num):
    """使用AI模型识别单页图片内容"""
    url = "https://api.siliconflow.cn/v1/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "请识别并提取这张图片中的所有文字内容，以排版清爽的md格式输出，不要添加任何额外说明，适当修复错别字，不要识别页码，页眉等无关信息，如果有插图，则详细描述，如果遇到数学公式等，则使用 LaTeX 语法来表示，表格也使用md格式。"
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{base64_image}"
                        }
                    }
                ]
            }
        ],
        "stream": False
    }
    
    # 重试机制
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=3000)
            response.raise_for_status()
            result = response.json()
            return page_num, result['choices'][0]['message']['content'], True
        except requests.exceptions.RequestException as e:
            print(f"😅 第{page_num}页请求错误 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"响应内容: {e.response.text}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_INTERVAL * 2)  
        except KeyError as e:
            print(f"😫 第{page_num}页响应格式错误 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(REQUEST_INTERVAL * 2)
    
    return page_num, None, False


def convert_page_to_image(args):
    """将PDF单页转换为图片"""
    pdf_document, page_num, temp_image_dir, dpi = args
    try:
        page = pdf_document[page_num]
        pix = page.get_pixmap(dpi=dpi)
        image_path = os.path.join(temp_image_dir, f"page_{page_num + 1}.png")
        pix.save(image_path)
        return (image_path, page_num + 1)
    except Exception as e:
        print(f"😨 转换第{page_num + 1}页为图片时出错: {e}")
        return None


def process_page(page_data, pdf_name, output_dir):
    """处理单页：编码并OCR，并立即保存结果"""
    image_path, page_num = page_data
    
    # 检查是否已经存在对应的txt文件
    output_file = os.path.join(output_dir, f"{pdf_name}-{page_num}.txt")
    if os.path.exists(output_file):
        print(f"⏩ 跳过第{page_num}页，OCR结果已存在")
        return page_num, "SKIPPED"
    
    base64_image = encode_image_to_base64(image_path)
    # 添加适当延迟以避免触发速率限制
    time.sleep(REQUEST_INTERVAL)
    
    page_num, ocr_result, success = ocr_image_with_ai_model(base64_image, page_num)
    
    if success and ocr_result:
        # 立即保存结果
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(ocr_result)
            print(f"✅ 第{page_num}页OCR识别完成，结果已保存到 {output_file}")
        except Exception as e:
            print(f"❌ 保存第{page_num}页结果失败: {e}")
            return page_num, None
        return page_num, ocr_result
    else:
        print(f"❌ 第{page_num}页OCR识别失败，已达到最大重试次数 {MAX_RETRIES}")
        return page_num, None


def main():
    # 检查API密钥
    if not API_KEY:
        print("😥 错误: 请设置API_KEY环境变量或者修改程序顶部的API_KEY变量")
        return
    
    # 检查PDF文件是否存在
    if not os.path.exists(PDF_FILE):
        print(f"🤨 错误: 找不到PDF文件 {PDF_FILE}，请确定文件真的在那里……或者看看程序顶部的PDF_FILE变量是不是空着")
        return
    
    # 检查PyMuPDF库是否安装
    if not PYMUPDF_AVAILABLE:
        print("😫 错误: 缺少依赖库 PyMuPDF，请运行以下命令安装:")
        print("  pip install pymupdf")
        return
    
    # 创建输出目录
    pdf_name = os.path.splitext(PDF_FILE)[0]
    output_dir = pdf_name
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 创建临时图片目录
    temp_image_dir = os.path.join(output_dir, "temp_images")
    if not os.path.exists(temp_image_dir):
        os.makedirs(temp_image_dir)
    
    print(f"🧐 正在将PDF文件 {PDF_FILE} 拆解为图片...")
    image_paths = []
    try:
        # 检查是否已有图片缓存
        cached_images = []
        for filename in os.listdir(temp_image_dir):
            if filename.endswith(".png"):
                page_num = int(filename.split("_")[1].split(".")[0])
                image_path = os.path.join(temp_image_dir, filename)
                cached_images.append((image_path, page_num))
        
        # 如果有缓存图片，使用缓存
        if cached_images:
            cached_images.sort(key=lambda x: x[1])  # 按页码排序
            image_paths = cached_images
            print(f"🤓 发现 {len(image_paths)} 个缓存图片，直接使用")
        else:
            # 打开PDF文档
            pdf_document = fitz.open(PDF_FILE)
            total_pages = len(pdf_document)
            print(f"😋 PDF文件共 {total_pages} 页，开始处理...")
            
            # 使用多线程将每一页转换为图片
            pdf_conversion_args = [(pdf_document, page_num, temp_image_dir, 200) for page_num in range(total_pages)]
            
            with ThreadPoolExecutor(max_workers=min(16, (total_pages // 2) + 1)) as executor:
                # 提交所有转换任务
                future_to_page = {executor.submit(convert_page_to_image, args): args[1] for args in pdf_conversion_args}
                
                # 收集结果
                for future in as_completed(future_to_page):
                    result = future.result()
                    if result:
                        image_paths.append(result)
            
            # 按页码排序
            image_paths.sort(key=lambda x: x[1])
    except Exception as e:
        print(f"😒 PDF文件转换为图片失败: {e}")
        return
    
    # 使用线程池处理所有页面，并立即保存结果
    results = {}
    failed_pages = []
    skipped_pages = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # 提交所有任务
        future_to_page = {executor.submit(process_page, page_data, pdf_name, output_dir): page_data for page_data in image_paths}
        
        # 收集结果
        for future in as_completed(future_to_page):
            page_num, ocr_result = future.result()
            if ocr_result == "SKIPPED":
                skipped_pages.append(page_num)
            elif ocr_result:
                results[page_num] = ocr_result
            else:
                failed_pages.append(page_num)
    
    # 输出处理结果统计
    print(f"✅ 处理完成，成功处理 {len(results)} 页")
    if skipped_pages:
        print(f"⏩ 跳过 {len(skipped_pages)} 页（已存在OCR结果）: {skipped_pages}")
    if failed_pages:
        print(f"❌ {len(failed_pages)} 页处理失败: {failed_pages}")
    else:
        print("🎉 所有页面都处理成功")
    
    # 临时图片文件保留作为缓存
    print("✅ 所有页面处理完成，临时图片已保留作为缓存")

if __name__ == "__main__":
    main()