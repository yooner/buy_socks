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

# ==================== 新策略配置 ====================
# 买入规则1：DIF连续N天向0靠近（负数绝对值越来越小）
DIF_CONVERGENCE_DAYS = 3  # 连续N天向0靠近，默认3天

# 买入规则2：下降阶段结束后，DIF连续M天变大
DIF_INCREASE_AFTER_DOWNTREND_END_DAYS = 2  # 下降阶段结束后连续N天DIF变大，默认2天

# 卖出规则：DIF和MACD都从大变小
# 通过比较当前值与前一天的值来判断是否变小


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
    
    # 计算DIF向0靠近的连续天数
    df = calculate_dif_convergence_count(df)

    # 使用市场状态分析器获取原始市场状态
    analyzer = MarketStateAnalyzer(
        six_points_func=get_six_points_by_price,
        three_points_func=get_three_points_by_price
    )
    df = analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 提取状态转换信息和阶段结束标志
    state_transitions = []
    for i in range(len(df)):
        transition_info = ""
        if i > 0:
            prev_state = df.loc[i-1, 'market_state'] if 'market_state' in df.columns else ""
            curr_state = df.loc[i, 'market_state'] if 'market_state' in df.columns else ""
            is_segment_start = df.loc[i, 'is_segment_start'] if 'is_segment_start' in df.columns else False
            state_notes = df.loc[i, 'state_notes'] if 'state_notes' in df.columns else ""
            
            # 状态转换信息
            if is_segment_start and state_notes:
                transition_info = state_notes
            
            # 检查阶段结束标志
            # 上升阶段结束标志
            prev_uptrend_end = df.loc[i-1, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            curr_uptrend_end = df.loc[i, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            if not prev_uptrend_end and curr_uptrend_end:
                if transition_info:
                    transition_info += " | 上升阶段结束"
                else:
                    transition_info = "上升阶段结束"
            
            # 下降阶段结束标志
            prev_downtrend_end = df.loc[i-1, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns else False
            curr_downtrend_end = df.loc[i, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns else False
            if not prev_downtrend_end and curr_downtrend_end:
                if transition_info:
                    transition_info += " | 下降阶段结束"
                else:
                    transition_info = "下降阶段结束"
        else:
            transition_info = "初始状态"
        
        state_transitions.append(transition_info)
    
    df['state_transition'] = state_transitions
    
    return df


def calculate_dif_convergence_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算DIF向0靠近的连续天数
    
    逻辑：
    - 如果当天DIF比前一天更靠近0（绝对值更小），计数+1
    - 如果当天DIF比前一天远离0（绝对值更大），计数复位为1（当天作为第一天）
    - 只有在DIF为负数时才计算
    
    Args:
        df: DataFrame包含DIF数据
        
    Returns:
        DataFrame: 添加'dif_convergence_count'列
    """
    df = df.copy()
    df['dif_convergence_count'] = 0
    
    for i in range(len(df)):
        if i == 0:
            df.loc[i, 'dif_convergence_count'] = 0
            continue
            
        current_dif = df.loc[i, 'DIF']
        prev_dif = df.loc[i-1, 'DIF']
        
        # 只在DIF为负数时计算
        if pd.isna(current_dif) or current_dif >= 0:
            df.loc[i, 'dif_convergence_count'] = 0
            continue
        
        if pd.isna(prev_dif):
            df.loc[i, 'dif_convergence_count'] = 1 if current_dif < 0 else 0
            continue
        
        # 检查是否比前一天更靠近0（绝对值更小）
        if abs(current_dif) < abs(prev_dif):
            # 更靠近0，计数+1
            df.loc[i, 'dif_convergence_count'] = df.loc[i-1, 'dif_convergence_count'] + 1
        else:
            # 远离0，复位为1（当天作为第一天）
            df.loc[i, 'dif_convergence_count'] = 1
    
    return df


def check_dif_convergence_buy_signal(df: pd.DataFrame, current_idx: int, n_days: int = 3) -> bool:
    """
    检查DIF连续N天向0靠近的买入信号
    
    条件：
    - DIF为负数（在0以下）
    - 连续N天向0靠近（dif_convergence_count >= n_days）
    
    Args:
        df: DataFrame包含DIF数据
        current_idx: 当前索引
        n_days: 连续天数，默认3天
        
    Returns:
        bool: 是否触发买入信号
    """
    if current_idx < 1:
        return False
    
    # 检查当前DIF是否为负数
    current_dif = df.loc[current_idx, 'DIF']
    if pd.isna(current_dif) or current_dif >= 0:
        return False
    
    # 检查连续向0靠近的天数是否达到N天
    convergence_count = df.loc[current_idx, 'dif_convergence_count']
    if pd.isna(convergence_count):
        return False
    
    return convergence_count >= n_days


def check_macd_sell_signal(df: pd.DataFrame, current_idx: int) -> bool:
    """
    检查MACD卖出信号
    
    条件：
    - DIF从大变小（当前DIF < 前一天DIF）
    - MACD从大变小（当前MACD < 前一天MACD）
    
    Args:
        df: DataFrame包含DIF和MACD数据
        current_idx: 当前索引
        
    Returns:
        bool: 是否触发卖出信号
    """
    if current_idx < 1:
        return False
    
    current_dif = df.loc[current_idx, 'DIF']
    prev_dif = df.loc[current_idx - 1, 'DIF']
    current_macd = df.loc[current_idx, 'MACD']
    prev_macd = df.loc[current_idx - 1, 'MACD']
    
    if pd.isna(current_dif) or pd.isna(prev_dif) or pd.isna(current_macd) or pd.isna(prev_macd):
        return False
    
    # DIF和MACD都从大变小
    dif_decreasing = current_dif < prev_dif
    macd_decreasing = current_macd < prev_macd
    
    return dif_decreasing and macd_decreasing


def check_dif_increase_after_downtrend_end(df: pd.DataFrame, current_idx: int, n_days: int = 2, max_observation_days: int = 10) -> tuple:
    """
    检查下降阶段结束后DIF连续N天变大的买入信号
    
    条件：
    - 下降阶段结束标志触发后进入观察期
    - 在观察期内，如果DIF连续N天变大，则触发买入
    - 观察期结束后仍未触发则放弃
    
    Args:
        df: DataFrame包含DIF和下降阶段结束标志数据
        current_idx: 当前索引
        n_days: 连续天数，默认2天
        max_observation_days: 最大观察天数，默认10天
        
    Returns:
        tuple: (是否触发买入信号, 是否开始新的观察期, 观察期起始索引)
    """
    if current_idx < 1:
        return False, False, None
    
    # 检查当天是否触发了下降阶段结束标志
    curr_triggered = df.loc[current_idx, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns else False
    prev_triggered = df.loc[current_idx - 1, 'downtrend_end_flag_triggered'] if 'downtrend_end_flag_triggered' in df.columns and current_idx > 0 else False
    
    # 如果当天刚触发下降阶段结束标志，开始新的观察期
    if curr_triggered and not prev_triggered:
        return False, True, current_idx
    
    return False, False, None


def check_dif_increase_in_observation(df: pd.DataFrame, current_idx: int, observation_start_idx: int, n_days: int = 2, max_observation_days: int = 10) -> bool:
    """
    在观察期内检查DIF是否连续N天变大
    
    Args:
        df: DataFrame包含DIF数据
        current_idx: 当前索引
        observation_start_idx: 观察期起始索引（下降阶段结束标志触发日）
        n_days: 需要连续变大的天数
        max_observation_days: 最大观察天数
        
    Returns:
        bool: 是否触发买入信号
    """
    if observation_start_idx is None or current_idx <= observation_start_idx:
        return False
    
    # 检查是否在观察期内
    days_since_observation_start = current_idx - observation_start_idx
    if days_since_observation_start > max_observation_days:
        return False  # 观察期已过
    for j in range(n_days):
        check_idx = current_idx - j
        if check_idx < 1:
            if current_idx == 897:
                with open('debug_log.txt', 'a') as f:
                    f.write(f"  j={j}, check_idx={check_idx} < 1, return False\n")
            return False
        
        current_dif = df.loc[check_idx, 'DIF']
        prev_dif = df.loc[check_idx - 1, 'DIF']
        
        if pd.isna(current_dif) or pd.isna(prev_dif):
            if current_idx == 897:
                with open('debug_log.txt', 'a') as f:
                    f.write(f"  j={j}, check_idx={check_idx}, NaN detected, return False\n")
            return False
        
        # DIF必须变大（当前 > 前一天）
        if current_dif <= prev_dif:
            return False
    
    return True


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数（带交易逻辑）"""
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行回测
    return _run_backtest_with_trading(stock_code, df)


def _run_backtest_with_trading(stock_code: str, df: pd.DataFrame):
    """带交易逻辑的回测"""
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

    # 初始化交易状态
    position = 0  # 持仓数量
    cash = 100000  # 初始资金
    buy_price = 0  # 买入价格
    buy_type = None  # 买入类型：'A' = DIF向0靠近买入, 'B' = 下降阶段结束后买入
    trades = []  # 交易记录
    
    # 初始化观察期状态（用于买入信号2）
    observation_active = False  # 是否在观察期内
    observation_start_idx = None  # 观察期起始索引
    observation_buy_triggered = False  # 观察期内是否已触发买入
    
    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10} {'持仓':>6} {'操作':>8}"
    log_print(header)
    log_print("-" * 110)
    
    # 遍历每一天进行交易逻辑
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
        allow_buy = row['allow_buy_down_to_rally'] if 'allow_buy_down_to_rally' in row and pd.notna(row['allow_buy_down_to_rally']) else True
        
        action = ""  # 操作标记
        
        # 检查是否在下降趋势中（下降趋势不交易）
        is_downtrend = market_state == "下降趋势"
        
        # 检查买入信号1：DIF连续N天向0靠近（下降趋势不买入，下行破前低不买入）
        buy_signal_1 = False
        buy_signal_2 = False
        
        # 检查是否触发下降阶段结束标志，开始观察期
        signal_2_triggered, start_observation, obs_idx = check_dif_increase_after_downtrend_end(df, i)
        if start_observation:
            observation_active = True
            observation_start_idx = obs_idx
            observation_buy_triggered = False
        
        # 检查观察期是否过期
        if observation_active and observation_start_idx is not None:
            days_in_observation = i - observation_start_idx
            if days_in_observation > 10:  # 最大观察期10天
                observation_active = False
                observation_start_idx = None
        
        # 买入信号1：DIF连续向0靠近（需要不在下降趋势）
        if position == 0 and not is_downtrend and allow_buy:
            buy_signal_1 = check_dif_convergence_buy_signal(df, i, DIF_CONVERGENCE_DAYS)
            if buy_signal_1:
                # 执行买入A
                buy_price = close_price
                position = int(cash / buy_price / 100) * 100  # 整手买入
                if position > 0:
                    cash -= position * buy_price
                    buy_type = 'A'  # 标记为买入A
                    action = f"买入A@{buy_price:.2f}"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': buy_price,
                        'quantity': position
                    })
        
        # 买入信号2：在观察期内，DIF连续变大（独立于allow_buy限制，只要不在下降趋势）
        if position == 0 and not is_downtrend and observation_active and not observation_buy_triggered and observation_start_idx is not None:
            buy_signal_2 = check_dif_increase_in_observation(df, i, observation_start_idx, DIF_INCREASE_AFTER_DOWNTREND_END_DAYS)
            if buy_signal_2:
                # 执行买入B
                buy_price = close_price
                position = int(cash / buy_price / 100) * 100  # 整手买入
                if position > 0:
                    cash -= position * buy_price
                    buy_type = 'B'  # 标记为买入B
                    action = f"买入B@{buy_price:.2f}"
                    observation_buy_triggered = True  # 标记观察期内已买入
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': buy_price,
                        'quantity': position
                    })
        
        # 检查卖出信号
        elif position > 0:  # 持仓时才考虑卖出
            sell_signal = False
            
            # 获取上升阶段结束标志
            uptrend_end_triggered = row['uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in row and pd.notna(row['uptrend_end_flag_triggered']) else False
            
            if buy_type == 'B':
                # 买入B（下降阶段结束后买入）：等待上升阶段结束标志触发后卖出，或变为下降趋势时卖出
                if uptrend_end_triggered or is_downtrend:
                    sell_signal = True
            else:
                # 买入A（DIF向0靠近买入）：使用MACD卖出信号
                if check_macd_sell_signal(df, i):
                    sell_signal = True
            
            if sell_signal:
                # 执行卖出
                sell_price = close_price
                cash += position * sell_price
                profit = (sell_price - buy_price) * position
                profit_pct = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                action = f"卖出@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"
                trades.append({
                    'day': day_num,
                    'date': date_str,
                    'action': '卖出',
                    'price': sell_price,
                    'quantity': position,
                    'profit': profit,
                    'profit_pct': profit_pct
                })
                position = 0
                buy_price = 0
                buy_type = None  # 重置买入类型
        
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
        
        # 持仓和操作建议
        line += f" {position:>6} {action:>8}"
        
        log_print(line)
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    total_return = (final_value - 100000) / 100000 * 100
    
    log_print(f"\n{'='*110}")
    log_print(f"【回测完成 - 新策略】")
    log_print(f"初始资金: 100,000")
    log_print(f"最终资产: {final_value:,.2f}")
    log_print(f"总收益率: {total_return:+.2f}%")
    log_print(f"交易次数: {len(trades)}")
    
    # 打印交易明细
    if trades:
        log_print(f"\n{'='*110}")
        log_print("【交易明细】")
        for trade in trades:
            if trade['action'] == '买入':
                log_print(f"  买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            else:
                log_print(f"  卖出: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
    
    log_print(f"{'='*110}")
    
    # 写入文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return final_value, trades, {'trades': trades, 'final_value': final_value, 'holding_info': position if position > 0 else None}


if __name__ == "__main__":
    run_backtest()
