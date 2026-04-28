"""
波动率策略 - 基于波动率变化的交易策略（数据展示版，无交易逻辑）
"""


import pandas as pd
import numpy as np
import os
import talib
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
)
from market_state_analyzer import (
    MarketStateAnalyzer,
    get_six_points_by_price,
    get_three_points_by_price
)


def get_output_file_path(base_name="out_put.txt"):
    """获取可用的输出文件路径，如果被占用则使用序号递增"""
    if not os.path.exists(base_name):
        return base_name
    
    # 尝试写入测试
    try:
        with open(base_name, 'w', encoding='utf-8') as f:
            pass
        return base_name
    except (PermissionError, IOError):
        # 文件被占用，寻找可用的序号
        base, ext = os.path.splitext(base_name)
        counter = 1
        while True:
            new_path = f"{base}{counter}{ext}"
            if not os.path.exists(new_path):
                return new_path
            try:
                with open(new_path, 'w', encoding='utf-8') as f:
                    pass
                return new_path
            except (PermissionError, IOError):
                counter += 1
                if counter > 100:  # 防止无限循环
                    raise Exception("无法找到可用的输出文件路径")


# ATR周期配置
ATR_PERIOD = 14  # ATR计算周期，可配置为10、14、20等


def calculate_slope_atr(df, ma_period=20, atr_period=14, n=5):
    """计算波动率指标"""
    df = df.copy()
    
    # 计算MA
    ma_col = f'ma{ma_period}'
    df[ma_col] = df['收盘'].rolling(window=ma_period, min_periods=ma_period).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = df['tr'].rolling(window=atr_period, min_periods=atr_period).mean()
    
    # 计算波动率：(收盘价 - MA) / ATR
    df['波动率'] = ((df['收盘'] - df[ma_col]) / df['atr']).replace([np.inf, -np.inf], np.nan)
    
    return df


def prepare_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据，计算所有技术指标"""
    df = df.copy()
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 只取最近一年的数据
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)
    
    # 计算技术指标
    df['ma20'] = df['收盘'].rolling(window=20, min_periods=20).mean()
    
    # 计算ATR
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()
    
    # 计算波动率
    df = calculate_slope_atr(df, ma_period=20, atr_period=ATR_PERIOD, n=5)

    # 使用TA-Lib计算MACD指标 (默认参数: fastperiod=12, slowperiod=26, signalperiod=9)
    df['DIF'], df['DEA'], df['MACD'] = talib.MACD(
        df['收盘'].values,
        fastperiod=12,
        slowperiod=26,
        signalperiod=9
    )
    # MACD柱状图通常显示为2倍
    df['MACD'] = df['MACD'] * 2

    # 使用市场状态分析器获取原始市场状态
    analyzer = MarketStateAnalyzer(
        six_points_func=get_six_points_by_price,
        three_points_func=get_three_points_by_price
    )
    df = analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 提取状态转换信息
    state_transitions = []
    for i in range(len(df)):
        if i > 0:
            prev_state = df.loc[i-1, 'market_state'] if 'market_state' in df.columns else ""
            curr_state = df.loc[i, 'market_state'] if 'market_state' in df.columns else ""
            is_segment_start = df.loc[i, 'is_segment_start'] if 'is_segment_start' in df.columns else False
            state_notes = df.loc[i, 'state_notes'] if 'state_notes' in df.columns else ""
            if is_segment_start and state_notes:
                state_transitions.append(state_notes)
            else:
                state_transitions.append("")
        else:
            state_transitions.append("初始状态")
    
    df['state_transition'] = state_transitions
    
    return df


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数（数据展示版，无交易逻辑）"""
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行数据展示
    return _run_data_display(stock_code, df)


def _run_data_display(stock_code: str, df: pd.DataFrame):
    """数据展示（无交易逻辑）"""
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 打印表头
    log_print(f"\n{'='*175}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"{'='*165}\n")

    header = f"{'日':<5} {'日期':<8} {'收盘':>6} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10}"
    log_print(header)
    log_print("-" * 90)
    
    # 遍历每一天进行数据展示
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        
        close_price = row['收盘']
        volatility = row['波动率']
        atr = row['atr']
        dif = row['DIF']
        dea = row['DEA']
        macd = row['MACD']
        market_state = row['market_state'] if pd.notna(row['market_state']) else ""
        state_transition = row['state_transition'] if pd.notna(row['state_transition']) else ""
        
        # 格式化输出
        line = f"{day_num:<5} {date_str:<10} {close_price:>8.2f}"
        
        # ATR
        if pd.notna(atr):
            line += f" {atr:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # 波动率
        if pd.notna(volatility):
            line += f" {volatility:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # DIF
        if pd.notna(dif):
            line += f" {dif:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # DEA
        if pd.notna(dea):
            line += f" {dea:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # MACD
        if pd.notna(macd):
            line += f" {macd:>8.2f}"
        else:
            line += f" {'N/A':>8}"
        
        # 市场状态
        if state_transition:
            line += f" {market_state:>10}|{state_transition}"
        else:
            line += f" {market_state:>10}"
        
        log_print(line)
    
    log_print(f"\n{'='*175}")
    log_print("【数据展示完成 - 无交易逻辑】")
    log_print(f"{'='*175}")
    
    # 写入文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return 0, {}, {'trades': [], 'final_value': 0, 'holding_info': None}


if __name__ == "__main__":
    run_backtest()
