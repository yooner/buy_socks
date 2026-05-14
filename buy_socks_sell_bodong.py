"""
波动率策略 - 基于市场状态转换的交易策略
"""


import pandas as pd
import numpy as np
import os
import sys
import subprocess
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

# MACD卖出阈值
MACD_SELL_THRESHOLD = 0.03  # MACD从大变小的幅度阈值

# MACD卖出阈值 - 上升趋势
MACD_SELL_THRESHOLD_UPTREND = -0.15

# 自然回升MACD高值保护阈值 - 当MACD大于此值时，即使变低也不卖出
NATURAL_RALLY_MACD_PROTECT_THRESHOLD = 1.0  # 可配置，默认1.0

MAX_OBSERVATION_DAYS = 3  # 最大观察天数

def check_macd_sell_signal(df: pd.DataFrame, current_idx: int, threshold: float = 0.05) -> bool:
    """
    检查MACD卖出信号
    
    条件：
    - MACD柱从大变小（当前MACD < 前一天MACD）
    
    Args:
        df: DataFrame包含MACD数据
        current_idx: 当前索引
        threshold: MACD变小的幅度阈值（已弃用，保留参数兼容性）
        
    Returns:
        bool: 是否触发卖出信号
    """
    if current_idx < 1:
        return False
    
    current_macd = df.loc[current_idx, 'MACD']
    prev_macd = df.loc[current_idx - 1, 'MACD']
    
    if pd.isna(current_macd) or pd.isna(prev_macd):
        return False
    
    # MACD柱从大变小（当前 < 前一天），不限制幅度
    macd_decreasing = current_macd < prev_macd
    
    return macd_decreasing


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
            # 上升阶段结束标志（DIF判断已内化到market_state_analyzer中）
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


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    
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
    trades = []  # 交易记录
    
    # 定义上升类型和下降类型的趋势
    rising_trends = ['上升趋势', '自然回升', '次级回升']
    falling_trends = ['下降趋势', '自然回撤', '次级回撤']
    
    # 观察期变量：用于从下降类型转为上升类型时，MACD为负的情况
    observing_macd_positive = False  # 是否在观察MACD变正
    observation_start_day = 0  # 观察开始的天数索引
    
    # 观察期变量：用于从自然回撤→上升趋势时，等待价格超过触发点
    observing_natural_reaction_to_uptrend = False  # 是否在观察自然回撤→上升趋势的买入时机
    natural_reaction_trigger_price = 0  # 自然回撤→上升趋势的触发价格（状态转换时的价格）

    
    # 上升阶段恢复相关变量
    uptrend_end_price = 0  # 上升阶段结束时的价格
    uptrend_recovery_active = False  # 是否处于上升阶段恢复观察中
    SIX_POINTS_THRESHOLD = 0.20  # 6个点 = 20%涨幅
    is_recovery_buy = False  # 是否是上升阶段恢复买入的持仓（恢复买入后MACD为负不卖出）
    
    # 上升阶段恢复买入观察变量
    observing_recovery_buy = False  # 是否在观察上升阶段恢复买入时机
    recovery_buy_trigger_price = 0  # 恢复买入的触发价格（达到3个点时的价格）
    
    # 新增：上升趋势卖出后观察MACD从小变大再买入
    observing_macd_rebuy = False  # 是否在观察MACD从小变大再买入
    macd_rebuy_start_day = 0  # 观察开始的天数索引
    
    # 新增：上升趋势分步卖出相关变量
    uptrend_sell_stage = 0  # 上升趋势卖出阶段：0=未卖出，1=已卖50%，2=已清仓
    consecutive_decline_days = 0  # 连续下降天数计数
    
    # 新增：MACD再买入观察期间的连续增长天数计数
    macd_rebuy_growth_count = 0  # MACD连续增长天数计数（用于再买入观察）
    
    # 新增：下降趋势→自然回升的DIF/MACD观察变量
    DIF_MACD_GROWTH_DAYS = 3  # DIF与MACD连续向上增长的天数（可配置）
    DIF_MACD_GROWTH_THRESHOLD = 0.20  # 增长幅度阈值
    observing_dif_macd_growth = False  # 是否在观察DIF与MACD连续增长
    dif_macd_growth_start_day = 0  # 观察开始的天数索引
    
    # 新增：防止卖出当天再买入的标志
    just_sold_today = False  # 当天是否刚卖出
    dif_macd_growth_first_dif = 0  # 第一天的DIF值
    dif_macd_growth_first_macd = 0  # 第一天的MACD值
    natural_rally_buy_triggered = False  # 自然回升买入是否已触发（买入后不再止损）
    just_bought_today = False  # 标记今天是否刚买入（避免当天买入又卖出）
    
    # 新增：记录前一个上升趋势的高点
    prev_uptrend_high = 0  # 前一个上升趋势的高点价格
    dif_macd_growth_count = 0  # DIF与MACD连续增长天数计数
    
    # 新增：记录前一个下降趋势的低点（用于新策略）
    prev_downtrend_low = 0  # 前一个下降趋势的低点价格
    current_downtrend_low = 0  # 当前下降趋势的低点价格（用于判断同一下降趋势）
    downtrend_buy_done = False  # 当前下降趋势是否已经买入过（避免同一趋势多次买入）
    
    # 新增：买入类型标记（用于区分两种不同的买入情况）
    buy_type = 0  # 0=无持仓，1=第一种买入（下降未破前低），2=第二种买入（下降突破前低后在上升买入）
    
    # 新增：记录当前持仓的高点（用于连续回落判断）
    position_high = 0  # 当前持仓期间的最高价格
    consecutive_fall_days = 0  # 从高点连续回落天数
    
    # 新增：记录买入时的前高（用于判断是否突破前高）
    buy_prev_uptrend_high = 0  # 买入时的前一个上升趋势高点
    
    # 新增：类型3买入后是否已经进入下一个上升趋势
    type3_next_uptrend_started = False  # 类型3买入后，如果进入下一个上升趋势，设为True
    
    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10} {'持仓':>6} {'操作':>20}"
    log_print(header)
    log_print("-" * 130)
    
    # 遍历每一天进行交易逻辑
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        
        # 重置当天卖出标志和买入标志
        just_sold_today = False
        just_bought_today = False
        
        close_price = row['收盘']
        volatility = row['波动率']
        atr = row['atr']
        dif = row['DIF']
        dea = row['DEA']
        macd = row['MACD']
        market_state = row['market_state'] if pd.notna(row['market_state']) else ""
        state_transition = row['state_transition'] if pd.notna(row['state_transition']) else ""
        
        action = ""  # 操作标记
        
        # 获取前一天的市场状态（用于判断状态转换）
        prev_market_state = df.loc[i-1, 'market_state'] if i > 0 else ""
        
        # 判断当前状态和前一天状态的趋势类型
        curr_is_rising = market_state in rising_trends
        curr_is_falling = market_state in falling_trends
        prev_is_rising = prev_market_state in rising_trends if prev_market_state else False
        prev_is_falling = prev_market_state in falling_trends if prev_market_state else False
        
        # 记录前一个上升趋势的高点：当从上升趋势转为非上升趋势时，记录高点
        if prev_market_state == '上升趋势' and market_state != '上升趋势':
            # 找到前一个上升趋势期间的最高价格
            j = i - 1
            uptrend_high = df.loc[j, '收盘']
            while j > 0 and df.loc[j, 'market_state'] == '上升趋势':
                if df.loc[j, '收盘'] > uptrend_high:
                    uptrend_high = df.loc[j, '收盘']
                j -= 1
            prev_uptrend_high = uptrend_high
            
            # 如果是类型3买入，标记已经进入下一个上升趋势（从下一个上升趋势转为非上升时）
            if buy_type == 3 and not type3_next_uptrend_started:
                type3_next_uptrend_started = True
        
        # 记录前一个下降趋势的低点：当从下降趋势转为非下降趋势时，记录低点
        if prev_market_state == '下降趋势' and market_state != '下降趋势':
            # 找到前一个下降趋势期间的最低价格
            j = i - 1
            downtrend_low = df.loc[j, '收盘']
            while j > 0 and df.loc[j, 'market_state'] == '下降趋势':
                if df.loc[j, '收盘'] < downtrend_low:
                    downtrend_low = df.loc[j, '收盘']
                j -= 1
            prev_downtrend_low = downtrend_low
            current_downtrend_low = downtrend_low  # 记录当前下降趋势的低点
            downtrend_buy_done = False  # 新下降趋势开始，重置买入标志
        
        # ==========================================
        # 新策略：买入逻辑
        # ==========================================
        
        # 买入情况1：在下降趋势中，未突破前一个下降趋势的低点，MACD连续2天增长买入
        # 买入情况2：在下降趋势中，突破了前一个下降趋势的低点，在随后的上升趋势买入
        
        # 检查MACD是否连续增长（跨越市场状态）
        if position == 0:
            current_macd = df.loc[i, 'MACD']
            prev_macd = df.loc[i-1, 'MACD'] if i > 0 else None
            
            # 检查MACD是否连续增长（可以跨越下降趋势、自然回升、上升趋势）
            if pd.notna(current_macd) and pd.notna(prev_macd) and current_macd > prev_macd:
                macd_rebuy_growth_count += 1
            else:
                macd_rebuy_growth_count = 0
        
        if position == 0:
            # 检查MACD是否连续2天增长（可以跨越下降趋势、自然回升、上升趋势）
            if macd_rebuy_growth_count >= 2:
                # 判断当前市场状态和买入类型
                if market_state == '下降趋势':
                    # 情况1：在下降趋势中，未突破前一个下降趋势的低点，且同一下降趋势未买入过
                    if prev_downtrend_low > 0 and close_price > prev_downtrend_low and not downtrend_buy_done:
                        buy_price = close_price
                        position = int(cash / buy_price / 100) * 100
                        if position > 0:
                            cash -= position * buy_price
                            action = f"买入@{buy_price:.2f}(类型1:下降未破前低)"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入类型1',
                                'price': buy_price,
                                'quantity': position
                            })
                            buy_type = 1
                            position_high = close_price
                            consecutive_fall_days = 0
                            just_bought_today = True  # 标记当天已买入
                            downtrend_buy_done = True  # 标记同一下降趋势已买入
                            buy_prev_uptrend_high = prev_uptrend_high  # 记录买入时的前高
                            macd_rebuy_growth_count = 0  # 买入后重置计数
                elif market_state in ['自然回升', '上升趋势']:
                    # MACD连续增长跨越到自然回升或上升趋势，也按类型1买入
                    # 但需要检查前低条件（使用最近的下降趋势低点）且同一下降趋势未买入过
                    if prev_downtrend_low > 0 and close_price > prev_downtrend_low and not downtrend_buy_done:
                        buy_price = close_price
                        position = int(cash / buy_price / 100) * 100
                        if position > 0:
                            cash -= position * buy_price
                            action = f"买入@{buy_price:.2f}(类型1:MACD连增跨越)"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入类型1',
                                'price': buy_price,
                                'quantity': position
                            })
                            buy_type = 1
                            position_high = close_price
                            consecutive_fall_days = 0
                            just_bought_today = True  # 标记当天已买入
                            downtrend_buy_done = True  # 标记同一下降趋势已买入
                            buy_prev_uptrend_high = prev_uptrend_high  # 记录买入时的前高
                            macd_rebuy_growth_count = 0  # 买入后重置计数
            
            # 检查是否从下降趋势转为上升趋势（情况2的买入）
            if prev_market_state == '下降趋势' and market_state == '上升趋势':
                # 检查之前的下降趋势是否突破了前一个低点
                if prev_downtrend_low > 0:
                    # 找到当前下降趋势的最低点
                    j = i - 1
                    current_downtrend_low = df.loc[j, '收盘']
                    while j > 0 and df.loc[j, 'market_state'] == '下降趋势':
                        if df.loc[j, '收盘'] < current_downtrend_low:
                            current_downtrend_low = df.loc[j, '收盘']
                        j -= 1
                    
                    # 如果当前下降趋势突破了前一个下降趋势的低点
                    if current_downtrend_low <= prev_downtrend_low:
                        # 情况2：在上升趋势买入
                        buy_price = close_price
                        position = int(cash / buy_price / 100) * 100
                        if position > 0:
                            cash -= position * buy_price
                            action = f"买入@{buy_price:.2f}(类型2:突破前低后上升)"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入类型2',
                                'price': buy_price,
                                'quantity': position
                            })
                            buy_type = 2
                            position_high = close_price
                            consecutive_fall_days = 0
                            just_bought_today = True  # 标记当天已买入
                            buy_prev_uptrend_high = prev_uptrend_high  # 记录买入时的前高
        
        # ==========================================
        # 新策略：卖出逻辑
        # ==========================================
        
        # 更新持仓期间的最高价格
        # 1. 从上升类型转为下降类型时卖出
        # 2. 在自然回升或次级回升中，MACD从大变小卖出
        # 更新持仓期间的最高价格
        if position > 0 and close_price > position_high:
            position_high = close_price
            consecutive_fall_days = 0  # 重置连续回落天数
        
        # 检查是否从高点回落
        if position > 0 and close_price < position_high:
            consecutive_fall_days += 1
        
        # 卖出逻辑
        if position > 0 and not just_bought_today:
            sell_signal = False
            
            # 类型1买入的卖出条件
            if buy_type == 1:
                # 条件1：跌破了前一个下降趋势的低点
                if prev_downtrend_low > 0 and close_price <= prev_downtrend_low:
                    sell_signal = True
                
                # 条件2：变为下降趋势时卖出（但买入当天不卖出）
                elif market_state == '下降趋势':
                    sell_signal = True
                
                # 条件3：在上升趋势中买入后未突破前高，转为自然回撤时卖出
                elif market_state == '自然回撤':
                    if buy_prev_uptrend_high > 0 and position_high <= buy_prev_uptrend_high:
                        sell_signal = True
            
            # 类型2买入的卖出条件
            elif buy_type == 2:
                # 条件1：在上升趋势中买入后未突破前高，转为自然回撤时卖出
                if market_state == '自然回撤':
                    if buy_prev_uptrend_high > 0 and position_high <= buy_prev_uptrend_high:
                        sell_signal = True
                
                # 条件2：在上升趋势中买入后未突破前高，转为下降趋势时卖出
                elif market_state == '下降趋势':
                    # 检查是否未突破前高（买入后的最高价 <= 前高）
                    if buy_prev_uptrend_high > 0 and position_high <= buy_prev_uptrend_high:
                        sell_signal = True
            
            # 类型3买入（突破买入）的卖出条件
            elif buy_type == 3:
                # 如果已经进入下一个上升趋势，卖出条件跟类型1、2一样
                if type3_next_uptrend_started:
                    # 条件1：在上升趋势中买入后未突破前高，转为自然回撤时卖出
                    if market_state == '自然回撤':
                        if buy_prev_uptrend_high > 0 and position_high <= buy_prev_uptrend_high:
                            sell_signal = True
                    
                    # 条件2：转为下降趋势时卖出
                    elif market_state == '下降趋势':
                        sell_signal = True
                else:
                    # 还在突破买入的那个上升趋势中
                    # 条件1：在自然回撤时跌破前一个上升趋势的高点
                    if market_state == '自然回撤':
                        if prev_uptrend_high > 0 and close_price <= prev_uptrend_high:
                            sell_signal = True
                    
                    # 条件2：转为下降趋势时卖出
                    elif market_state == '下降趋势':
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
                just_sold_today = True  # 标记当天已卖出
                
                # 重置买入类型和相关变量
                buy_type = 0
                position_high = 0
                consecutive_fall_days = 0
                buy_prev_uptrend_high = 0  # 重置买入时的前高
                type3_next_uptrend_started = False  # 重置类型3买入的下一个上升趋势标志
        
        # ==========================================
        # 突破前高后的再买入逻辑
        # ==========================================
        # 在卖出后（position=0），如果价格突破了前高，则再次买入
        # 条件：1. 当天没有卖出；2. 价格突破前高；3. 在上升趋势中
        if position == 0 and not just_sold_today and market_state == '上升趋势' and prev_uptrend_high > 0 and close_price > prev_uptrend_high:
            # 突破前高，执行买入
            buy_price = close_price
            position = int(cash / buy_price / 100) * 100
            if position > 0:
                cash -= position * buy_price
                action = f"买入@{buy_price:.2f}(突破前高)"
                trades.append({
                    'day': day_num,
                    'date': date_str,
                    'action': '突破买入',
                    'price': buy_price,
                    'quantity': position
                })
                buy_type = 3  # 标记为突破买入类型
                position_high = close_price
                consecutive_fall_days = 0
                just_bought_today = True  # 标记当天已买入
                # 记录买入时的前高，如果没有前高记录，则使用买入价格作为参考
                buy_prev_uptrend_high = prev_uptrend_high if prev_uptrend_high > 0 else buy_price
        

        # 条件：在上升趋势中，处于恢复观察状态，价格从上升阶段结束时的价格上升3个点（固定数值）以上
        # 上升阶段恢复买入逻辑
        
        # 如果状态变为非上升趋势，取消恢复买入观察
        if market_state != '上升趋势' and observing_recovery_buy:
            observing_recovery_buy = False
            recovery_buy_trigger_price = 0
        
        if market_state == '上升趋势' and uptrend_recovery_active and uptrend_end_price > 0:
            # 3个点是固定数值（元），根据上升阶段结束时的价格计算（3个点 = 6个点的一半）
            six_points_value = get_six_points_by_price(uptrend_end_price)
            if six_points_value is None:
                six_points_value = uptrend_end_price * 0.20  # 如果获取不到，默认使用20%
            three_points_value = six_points_value / 2  # 3个点 = 6个点的一半
            price_increase = close_price - uptrend_end_price
            
            # 如果价格达到3个点，开始观察，记录触发价格
            if price_increase >= three_points_value and not observing_recovery_buy:
                observing_recovery_buy = True
                recovery_buy_trigger_price = close_price
                action = f"观察恢复买入(触发价:{recovery_buy_trigger_price:.2f})"
            
            # 如果已经在观察中，且价格超过触发点，执行买入
            if observing_recovery_buy and close_price > recovery_buy_trigger_price:
                buy_price = close_price
                buy_quantity = int(cash / buy_price / 100) * 100  # 整手买入
                if buy_quantity > 0:
                    cash -= buy_quantity * buy_price
                    position += buy_quantity  # 加仓
                    action = f"上升阶段恢复买入@{buy_price:.2f}"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '上升阶段恢复买入',
                        'price': buy_price,
                        'quantity': buy_quantity
                    })
                    uptrend_recovery_active = False  # 重置恢复观察状态
                    uptrend_end_price = 0
                    # 标记为恢复买入持仓
                    is_recovery_buy = True
                    # 重置观察状态
                    observing_recovery_buy = False
                    recovery_buy_trigger_price = 0
                    observing_dif_macd_growth = False  # 重置DIF/MACD观察状态
        
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
        
        # 市场状态和状态转换
        if state_transition:
            line += f" {market_state:>10}|{state_transition}"
        else:
            line += f" {market_state:>10}"
        
        # 持仓
        line += f" {position:>6}"
        
        # 操作
        line += f" {action:>20}"
        
        log_print(line)
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
    total_return = (final_value - 100000) / 100000 * 100
    
    log_print(f"\n{'='*130}")
    log_print(f"【回测完成】")
    log_print(f"初始资金: 100,000")
    log_print(f"最终资产: {final_value:,.2f}")
    log_print(f"总收益率: {total_return:+.2f}%")
    log_print(f"交易次数: {len(trades)}")
    
    # 打印交易明细
    if trades:
        log_print(f"\n{'='*130}")
        log_print("【交易明细】")
        for trade in trades:
            if trade['action'] == '买入':
                log_print(f"  买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '上升阶段恢复买入':
                log_print(f"  上升阶段恢复买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '自然回升买入':
                log_print(f"  自然回升买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '买入类型1':
                log_print(f"  买入类型1: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '买入类型2':
                log_print(f"  买入类型2: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '突破买入':
                log_print(f"  突破买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '卖出50%':
                log_print(f"  卖出50%: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            elif trade['action'] == '买回':
                log_print(f"  买回: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '卖出':
                log_print(f"  卖出: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            else:
                log_print(f"  {trade['action']}: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
    
    log_print(f"{'='*130}")
    
    # 写入文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return final_value, trades


if __name__ == "__main__":
    # 检查是否有 -c 参数（生成图表）
    generate_chart = '-c' in sys.argv or '--chart' in sys.argv
    
    # 运行回测
    result = run_backtest()
    
    # 如果指定了 -c 参数，自动生成图表并打开
    if result and generate_chart:
        print("\n[正在生成图表...]")
        try:
            # 调用 generate_standalone_chart.py 生成图表
            script_dir = os.path.dirname(os.path.abspath(__file__))
            chart_script = os.path.join(script_dir, 'generate_standalone_chart.py')
            
            if os.path.exists(chart_script):
                subprocess.run([sys.executable, chart_script], check=True)
                
                # 打开生成的HTML文件
                html_file = os.path.join(script_dir, 'stock_chart_standalone.html')
                if os.path.exists(html_file):
                    print(f"[正在打开图表: {html_file}]")
                    if os.name == 'nt':  # Windows
                        os.startfile(html_file)
                    elif os.name == 'posix':  # macOS/Linux
                        subprocess.run(['open', html_file])
                else:
                    print(f"[警告: 未找到图表文件 {html_file}]")
            else:
                print(f"[警告: 未找到图表生成脚本 {chart_script}]")
        except Exception as e:
            print(f"[生成图表时出错: {e}]")
