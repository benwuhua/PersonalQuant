# Environment Setup

PersonalQuant 当前优先支持的运行环境是：

- macOS Apple Silicon
- Python 3.11
- 已验证可用的 Qlib 虚拟环境：`~/.venvs/qlib`

## 推荐方式：直接复用已验证环境

如果本机已经有可用环境，直接执行：

```bash
source ~/.venvs/qlib-activate.sh
```

然后验证：

```bash
python -V
python -c "import qlib, akshare, lightgbm, pandas; print('env ok')"
```

## 从零创建环境

### 1. 创建虚拟环境

```bash
python3.11 -m venv ~/.venvs/personalquant
source ~/.venvs/personalquant/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 准备 Qlib 数据目录

默认配置读取：

```text
~/.qlib/qlib_data/cn_data_simple
```

如果你的数据目录不同，修改：

```text
config/config.yaml -> qlib.provider_uri
```

## 最小验证

在项目根目录执行：

```bash
python -m compileall src scripts
python scripts/build_dashboard_data.py
```

如果要跑完整主流程：

```bash
python scripts/run_weekly_pipeline.py
```

## 依赖说明

- `pandas`：数据处理
- `pyyaml`：配置读取
- `lightgbm`：量价 baseline 模型
- `qlib`：A 股研究数据与训练框架
- `scikit-learn`：模型工具链
- `requests`：公告抓取
- `pypdf`：PDF 正文抽取
- `akshare`：实时 A 股行情与成分股数据

## 注意事项

1. Qlib 与 LightGBM 在不同系统上的安装体验差异较大，优先建议固定 Python 3.11。
2. 当前项目默认产物目录 `data/outputs/`、`data/processed/`、`data/archives/` 不提交到 git。
3. 如果 PDF 正文抽取出现空内容，系统会自动回退到 `title_only`，这是预期保护行为，不是流水线失败。
4. 当前默认 `llm.provider=mock`，所以项目不依赖外部 LLM API 才能跑通基础链路。
