# 股票回测系统

基于 Python 的股票交易策略回测系统，支持多种交易策略的回测、结果对比和自动版本管理。

## 功能特性

- **多策略支持**: 支持阈值策略和趋势爆发策略，可灵活扩展新策略
- **自动化回测**: 自动获取股票数据，执行回测，生成详细报告
- **结果可视化**: 生成 Excel 报告，包含总收益率、年度收益率等关键指标
- **Git 集成**: 当策略表现显著改善时，自动创建 Git Tag 记录版本
- **灵活配置**: 支持命令行参数配置，可指定策略和股票范围

## 目录结构

```
socks/
├── ana_stocks.py              # 股票数据获取与缓存模块
├── buy_socks_sell_thresholds.py  # 阈值策略实现
├── buy_socks_sell_outbreak.py     # 趋势爆发策略实现
├── run_all_socks.py           # 通用回测脚本（主入口）
├── run_all_socks_thresholds.py    # 阈值策略专用脚本（已弃用）
├── cache/                     # 股票数据缓存目录
│   └── *_daily.json          # 日线数据缓存文件
├── strategy/                  # 策略引擎模块
│   ├── data_layer.py         # 数据层
│   ├── buy_engine.py         # 买入引擎
│   └── sell_engine.py        # 卖出引擎
└── socks_results_*.xlsx       # 回测结果文件
```

## 安装依赖

```bash
pip install tushare pandas openpyxl
```

注意：`openpyxl` 版本需 >= 3.1.5

```bash
pip install --upgrade openpyxl
```

## 使用方法

### 运行所有策略

```bash
python run_all_socks.py --all
```

### 指定策略运行

```bash
# 只运行阈值策略
python run_all_socks.py --strategies thresholds

# 只运行趋势爆发策略
python run_all_socks.py --strategies outbreak

# 运行多个策略
python run_all_socks.py --strategies thresholds,outbreak
```

### 指定股票运行

```bash
# 只回测指定股票
python run_all_socks.py --stocks 000002,603496
```

### 组合使用

```bash
# 运行指定策略和指定股票
python run_all_socks.py --strategies thresholds --stocks 000002,603496
```

## 策略说明

### 阈值策略 (thresholds)

根据 MA20（20周均线）相对于当前价格的百分比来确定买卖点。

**参数配置**:
- `buy_levels`: 买入档位，如 [-10, -20, -30]（价格低于 MA20 的百分比）
- `buy_ratios`: 对应买入比例，如 [0.3, 0.3, 0.4]
- `sell_thresholds`: 卖出阈值，如 [5, 10, 20]（价格高于 MA20 的百分比）
- `sell_ratios`: 对应卖出比例，如 [0.3, 0.3, 0.4]

### 趋势爆发策略 (outbreak)

基于 MA20 趋势判断的突破策略。

**买入条件**:
- MA20 连续 4 周上行
- 当前价格高于 MA20

**卖出条件**:
- MA20 由上行转为下行
- 持仓期间 MA20 持续下行

## 输出结果

### 控制台输出

```
============================================================
策略: 阈值策略
============================================================
策略参数:
  INITIAL_CAPITAL: 100000
  buy_levels: [-10, -20, -30]
  buy_ratios: [0.3, 0.3, 0.4]
  sell_thresholds: [5, 10, 20]
  sell_ratios: [0.3, 0.3, 0.4]
============================================================

============================================================
策略: 阈值策略 | 股票: 000002
============================================================
  ✅ 000002: 总收益率 +25.32%
     年度收益: 2023:+0.0%, 2024:+8.5%, 2025:+12.1%, 2026:+2.3%

结果已保存到 socks_results_thresholds.xlsx
```

### Excel 文件

生成 `socks_results_*.xlsx` 文件，包含以下列:

| 列名 | 说明 |
|------|------|
| 编号 | 股票代码 |
| 总收益率 | 总体收益率（百分比） |
| 2023年收益率 | 2023 年度收益率 |
| 2024年收益率 | 2024 年度收益率 |
| ... | 以此类推 |
| gittag | 关联的 Git Tag |

### Git Tag 机制

当某一策略的股票收益率改善比例 >= 60% 时，系统会自动创建 Git Tag:

```
thresholds_20260212_145026
outbreak_20260212_150000
```

Tag 命名格式: `{策略名}_{日期}_{时间}`

## 数据源

使用 Tushare 获取股票日线数据，数据自动缓存到 `cache/` 目录。

### 缓存管理

缓存文件命名格式: `{股票代码}_daily.json`

```bash
# 查看缓存的股票
ls cache/

# 清除单个股票缓存
rm cache/000002_daily.json

# 清除所有缓存
rm cache/*_daily.json
```

## 扩展新策略

1. 创建新的策略脚本（如 `buy_socks_new_strategy.py`）

2. 实现 `run_backtest` 函数，返回格式:
   ```python
   def run_backtest(stock_code: str):
       # 逻辑实现
       return total_return, yearly_returns  # 必须返回 tuple
   ```

3. 在 `run_all_socks.py` 的 `STRATEGIES` 字典中添加配置:
   ```python
   STRATEGIES = {
       'new_strategy': {
           'module': 'buy_socks_new_strategy',
           'params': ['INITIAL_CAPITAL'],  # 需要导入的参数
           'run_backtest': 'run_backtest',
           'name': '新策略名称',
           'excel_suffix': 'new_strategy'  # Excel 文件后缀
       }
   }
   ```

## 历史文件

- `run_all_socks_thresholds.py`: 早期版本，已被 `run_all_socks.py` 替代
- `stocks_results.xlsx`: 旧版结果文件，可删除

## 注意事项

1. 首次运行需要网络连接获取股票数据
2. Tushare 需要 token 配置（已在代码中预设）
3. 回测周期默认为 3 年（BACKTEST_YEARS = 3）
4. 起始资金默认为 10 万元（INITIAL_CAPITAL = 100000）
