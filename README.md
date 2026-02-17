# E-book Text Extraction

从 EPUB/PDF 电子书中提取文本并保存为 Markdown 文件。支持多模型 VLM API 负载均衡、Tesseract OCR、流水线并行处理、断点续传和优雅退出。

## 安装

```bash
pip install -r requirements.txt
```

还需安装 [Tesseract OCR](https://github.com/tesseract-ocr/tesseract)（如果使用 OCR 模式）。

## 快速开始

首次运行会自动生成配置文件 `extract_config.toml`，请填写 API Key 后再运行：

```bash
python extract_text.py
```

不带任何文件参数时，默认处理 input 目录下所有 epub/pdf 文件。

## 命令行参数

```
python extract_text.py [files] [-f FILE ...] [-m METHOD] [-w N] [--w-full] [--dpi N] [-c PATH]
```

| 参数 | 说明 |
|------|------|
| `files` | 位置参数，要处理的文件路径（可多个） |
| `-f, --file` | 指定文件路径（支持多个），文件名会在 input 目录下查找 |
| `-m, --method` | 处理方法：`auto` `auto_ai` `text` `ocr` `ai` `ask` |
| `-w, --workers` | 并行线程数 |
| `--w-full` | 使用最高线程数（CPU核心数×4，上限32） |
| `--dpi` | PDF 渲染 DPI（默认200，推荐扫描版用300） |
| `-c, --config` | 自定义配置文件路径（默认 `extract_config.toml`） |

### 使用示例

```bash
# 处理 input 目录下所有 epub/pdf（默认行为）
python extract_text.py

# 指定方法和线程数
python extract_text.py -m auto_ai -w 25

# 最高线程 + 高 DPI
python extract_text.py -m auto_ai --w-full --dpi 300

# 指定多个文件
python extract_text.py -f book1.pdf book2.pdf

# 自定义配置文件
python extract_text.py -c my_config.toml
```

## 处理方法说明

| 方法 | 说明 |
|------|------|
| `auto` | 默认。先提取文本，图片页用 Tesseract，失败再用 AI |
| `auto_ai` | 先提取文本，图片页直接用 AI（跳过 Tesseract） |
| `text` | 仅提取嵌入文本，跳过图片页 |
| `ocr` | 全部用 Tesseract OCR |
| `ai` | 全部用 AI 视觉模型 |
| `ask` | 文本提取不佳时询问用户是否用 AI |

## 配置文件

配置文件为 TOML 格式，结构如下：

```toml
[settings]
tesseract_cmd = "D:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe"
tesseract_lang = "chi_sim+eng"
min_text_len = 50      # 少于此字符数视为图片页
min_ocr_len = 20       # Tesseract 结果少于此视为失败
max_workers = 4        # 并行线程数（API 并发数）
dpi = 200              # PDF 渲染 DPI
save_interval = 10     # 每 N 页自动保存进度

[api]
key = "your-api-key"
base_url = "https://api.siliconflow.cn/v1"
# 多模型轮询负载均衡
models = [
    "zai-org/GLM-4.6V",
    "Qwen/Qwen3-VL-32B-Instruct"
]

[file_dirs]
input_dir = "./input"
output_dir = "./output"
```

### 文件级配置

可以为单个文件指定处理方法和页码覆盖：

```toml
[[files]]
name = "扫描版书籍.pdf"
method = "ai"

[[files]]
name = "混合文档.pdf"
method = "auto"
[[files.overrides]]
pages = "1-3,50-55"
method = "ai"
```

## 断点续传

- 处理过程中每完成一定页数（默认10页）会自动保存进度到 `.progress.json` 文件
- Ctrl+C 中断时也会保存当前进度
- 再次运行同一文件时自动跳过已完成的页面
- API 失败（429限流、连接错误等）的页面不会被标记为已完成，下次运行自动重试
- 全部完成后自动删除进度文件

## 优雅退出

按 Ctrl+C 后程序会：
1. 停止投递新任务
2. 取消排队中的未执行任务
3. 等待正在执行的 API 请求完成（最多等几秒）
4. 保存当前进度
5. 退出

如果等不及，再按一次 Ctrl+C 会强制退出。

## 错误重试

API 调用遇到以下错误时会自动重试（最多3次），并切换到下一个模型：
- 429 限流错误
- 连接错误（Connection error）
- 请求超时（Timeout）

重试间隔递增（3s → 6s），避免持续触发限流。

## 性能建议

- `max_workers` 建议根据 API 限流设置，硅基流动推荐 10-20
- `--dpi 300` 会显著增加内存占用，建议配合较低的 workers 使用
- 多模型轮询可以有效分散限流压力
