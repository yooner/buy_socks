"""
波动率策略 - 基于市场状态转换的交易策略
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
    - MACD柱从大变小（当前MACD < 前一天MACD - threshold）
    
    Args:
        df: DataFrame包含MACD数据
        current_idx: 当前索引
        threshold: MACD变小的幅度阈值
        
    Returns:
        bool: 是否触发卖出信号
    """
    if current_idx < 1:
        return False
    
    current_macd = df.loc[current_idx, 'MACD']
    prev_macd = df.loc[current_idx - 1, 'MACD']
    
    if pd.isna(current_macd) or pd.isna(prev_macd):
        return False
    
    # MACD柱从大变小，且满足幅度阈值
    macd_decreasing = (prev_macd - current_macd) >= threshold
    
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
            # 上升阶段结束标志
            prev_uptrend_end = df.loc[i-1, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            curr_uptrend_end = df.loc[i, 'uptrend_end_flag_triggered'] if 'uptrend_end_flag_triggered' in df.columns else False
            # 特例：如果DIF > 1，不标记上升阶段结束
            current_dif = df.loc[i, 'DIF'] if 'DIF' in df.columns else None
            if not prev_uptrend_end and curr_uptrend_end:
                if pd.notna(current_dif) and current_dif > 1:
                    # DIF > 1，不标记上升阶段结束
                    pass
                else:
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

    
    # 上升阶段结束减仓相关变量
    reduce_position_active = False  # 是否处于减仓模式
    last_reduce_position_price = 0  # 上次减仓时的价格
    
    # 上升阶段恢复相关变量
    uptrend_end_price = 0  # 上升阶段结束时的价格
    uptrend_recovery_active = False  # 是否处于上升阶段恢复观察中
    SIX_POINTS_THRESHOLD = 0.20  # 6个点 = 20%涨幅
    is_recovery_buy = False  # 是否是上升阶段恢复买入的持仓（恢复买入后MACD为负不卖出）
    
    # 上升阶段恢复买入观察变量
    observing_recovery_buy = False  # 是否在观察上升阶段恢复买入时机
    recovery_buy_trigger_price = 0  # 恢复买入的触发价格（达到3个点时的价格）
    
    
    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10} {'持仓':>6} {'操作':>20}"
    log_print(header)
    log_print("-" * 130)
    
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
        
        action = ""  # 操作标记
        
        # 获取前一天的市场状态（用于判断状态转换）
        prev_market_state = df.loc[i-1, 'market_state'] if i > 0 else ""
        
        # 判断当前状态和前一天状态的趋势类型
        curr_is_rising = market_state in rising_trends
        curr_is_falling = market_state in falling_trends
        prev_is_rising = prev_market_state in rising_trends if prev_market_state else False
        prev_is_falling = prev_market_state in falling_trends if prev_market_state else False
        
        # 买入逻辑：从下降类型转为上升类型，或从上升类型转为上升类型时买入，且MACD>0
        if position == 0:
            # 条件1：从下降类型转为上升类型
            # 条件2：从上升类型转为上升类型（状态转换）
            is_falling_to_rising = prev_is_falling and curr_is_rising
            is_rising_to_rising = prev_is_rising and curr_is_rising and prev_market_state != market_state
            
            if is_falling_to_rising or is_rising_to_rising:
                current_macd = df.loc[i, 'MACD']
                
                # 特殊情况：从自然回撤→上升趋势，需要观察，等待价格超过触发点
                if prev_market_state == '自然回撤' and market_state == '上升趋势':
                    if pd.notna(current_macd) and current_macd > 0:
                        # MACD>0，开始观察，记录触发价格
                        observing_natural_reaction_to_uptrend = True
                        natural_reaction_trigger_price = close_price
                    else:
                        # MACD<=0，按照原有逻辑观察MACD变正
                        observing_macd_positive = True
                        observation_start_day = i
                
                # 情况1：MACD>0，直接买入（非自然回撤→上升趋势的情况）
                elif pd.notna(current_macd) and current_macd > 0:
                    buy_price = close_price
                    position = int(cash / buy_price / 100) * 100  # 整手买入
                    if position > 0:
                        cash -= position * buy_price
                        action = f"买入@{buy_price:.2f}"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'quantity': position
                        })
                        observing_macd_positive = False  # 重置观察状态
                
                # 情况2：从下降类型转为上升类型，且MACD<=0，开始观察
                elif is_falling_to_rising and pd.notna(current_macd) and current_macd <= 0:
                    observing_macd_positive = True
                    observation_start_day = i
            
            # 情况3：正在观察MACD变正（从下降类型转为上升类型的后续观察）
            elif observing_macd_positive:
                # 检查是否在观察期内（3天内）
                if i - observation_start_day <= MAX_OBSERVATION_DAYS:
                    current_macd = df.loc[i, 'MACD']
                    # MACD变正，执行买入
                    if pd.notna(current_macd) and current_macd > 0:
                        buy_price = close_price
                        position = int(cash / buy_price / 100) * 100  # 整手买入
                        if position > 0:
                            cash -= position * buy_price
                            action = f"买入@{buy_price:.2f}"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'quantity': position
                            })
                        observing_macd_positive = False  # 重置观察状态
                    # 如果状态又变回下降类型，取消观察
                    elif curr_is_falling:
                        observing_macd_positive = False
                else:
                    # 超过观察期，取消观察
                    observing_macd_positive = False
            
            # 情况4：正在观察自然回撤→上升趋势的买入时机（等待价格超过触发点）
            elif observing_natural_reaction_to_uptrend:
                # 如果状态变回下降类型，取消观察
                if curr_is_falling:
                    observing_natural_reaction_to_uptrend = False
                    natural_reaction_trigger_price = 0
                # 价格超过触发点，执行买入
                elif close_price > natural_reaction_trigger_price:
                    buy_price = close_price
                    position = int(cash / buy_price / 100) * 100  # 整手买入
                    if position > 0:
                        cash -= position * buy_price
                        action = f"买入@{buy_price:.2f}"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'quantity': position
                        })
                    observing_natural_reaction_to_uptrend = False  # 重置观察状态
                    natural_reaction_trigger_price = 0
            
        
        # 卖出逻辑：
        # 1. 从上升类型转为下降类型时卖出
        # 2. 在自然回升或次级回升中，MACD从大变小卖出
        # 3. 在上升趋势中，MACD变成负的卖出
        # 4. 上升阶段结束减仓逻辑
        
        # 检查是否触发上升阶段结束标志
        is_uptrend_end = '上升阶段结束' in state_transition if state_transition else False
        
        if position > 0:
            sell_signal = False
            reduce_position = False  # 是否减仓
            reduce_30_percent = False  # 是否减仓30%（特殊情况）
            
            # 情况0：上升趋势→自然回撤且DIF > 1，减仓30%
            if prev_market_state == '上升趋势' and market_state == '自然回撤':
                current_dif = df.loc[i, 'DIF']
                if pd.notna(current_dif) and current_dif > 1:
                    reduce_30_percent = True
                    uptrend_end_price = close_price  # 记录价格用于恢复买入计算
                    uptrend_recovery_active = True  # 开始观察上升阶段恢复
            
            # 情况1：遇到上升阶段结束标志，开始减仓模式，第一次减仓50%，记录结束价格
            if is_uptrend_end and not reduce_position_active:
                reduce_position = True
                reduce_position_active = True
                last_reduce_position_price = close_price
                uptrend_end_price = close_price  # 记录上升阶段结束时的价格
                uptrend_recovery_active = True  # 开始观察上升阶段恢复
            
            # 情况2：已经处于减仓模式，如果价格超过上次减仓价格，继续减仓
            elif reduce_position_active and close_price > last_reduce_position_price:
                reduce_position = True
                last_reduce_position_price = close_price
            
            # 执行减仓30%（特殊情况）
            if reduce_30_percent:
                # 减仓30%
                sell_quantity = int(position * 0.50 / 100) * 100  # 整手计算
                if sell_quantity < 100 and position >= 100:
                    sell_quantity = 100  # 至少卖100股
                remaining_quantity = position - sell_quantity
                if sell_quantity > 0:
                    sell_price = close_price
                    cash += sell_quantity * sell_price
                    profit = (sell_price - buy_price) * sell_quantity
                    profit_pct = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                    action = f"减仓30%@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '减仓30%',
                        'price': sell_price,
                        'quantity': sell_quantity,
                        'profit': profit,
                        'profit_pct': profit_pct
                    })
                    position = remaining_quantity
            
            # 执行减仓50%（正常情况）
            elif reduce_position:
                # 每次减仓当前持仓的50%，但如果持仓低于100股则全部卖出
                if position < 100:
                    # 低于100股，全部卖出
                    sell_quantity = position
                    remaining_quantity = 0
                    action_type = '减仓清仓'
                else:
                    # 正常减仓50%
                    sell_quantity = position // 2
                    remaining_quantity = position - sell_quantity
                    action_type = '减仓50%'
                
                if sell_quantity > 0:
                    sell_price = close_price
                    cash += sell_quantity * sell_price
                    profit = (sell_price - buy_price) * sell_quantity
                    profit_pct = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                    action = f"{action_type}@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': action_type,
                        'price': sell_price,
                        'quantity': sell_quantity,
                        'profit': profit,
                        'profit_pct': profit_pct
                    })
                    position = remaining_quantity
                    # 如果持仓减到0，退出减仓模式
                    if position == 0:
                        reduce_position_active = False
                        last_reduce_position_price = 0
                        buy_price = 0
            
            # 条件1：从上升类型转为下降类型，或从自然回撤转为下降趋势
            # 特例：如果DIF > 1，从上升趋势→自然回撤不卖出
            if prev_is_rising and curr_is_falling:
                current_dif = df.loc[i, 'DIF']
                if prev_market_state == '上升趋势' and market_state == '自然回撤' and pd.notna(current_dif) and current_dif > 1:
                    pass  # DIF > 1，上升趋势→自然回撤不卖出
                else:
                    sell_signal = True
            # 自然回撤→下降趋势也应该卖出
            elif prev_market_state == '自然回撤' and market_state == '下降趋势':
                sell_signal = True
            
            # 条件2：在自然回升或次级回升中，MACD从大变小
            # 特例：如果DIF > 1，自然回升中不卖出
            if market_state in ['自然回升', '次级回升']:
                current_macd = df.loc[i, 'MACD']
                current_dif = df.loc[i, 'DIF']
                # 自然回升特殊逻辑：如果DIF > 1，不卖出
                if market_state == '自然回升' and pd.notna(current_dif) and current_dif > 1:
                    pass  # DIF > 1，自然回升不卖出
                # 自然回升特殊逻辑：如果MACD大于保护阈值，即使变低也不卖出
                elif market_state == '自然回升' and pd.notna(current_macd) and current_macd >= NATURAL_RALLY_MACD_PROTECT_THRESHOLD:
                    pass  # MACD高值保护，不卖出
                elif check_macd_sell_signal(df, i, MACD_SELL_THRESHOLD):
                    sell_signal = True
            
            # 条件3：在上升趋势中，MACD < -0.15 时卖出
            # 特例：如果DIF > 1，MACD为负时不卖出（不止损）
            if market_state == '上升趋势':
                current_macd = df.loc[i, 'MACD']
                current_dif = df.loc[i, 'DIF']
                if pd.notna(current_macd) and current_macd < MACD_SELL_THRESHOLD_UPTREND:
                    # 如果DIF > 1，不卖出（适用于所有持仓类型）
                    if pd.notna(current_dif) and current_dif > 1:
                        pass  # DIF > 1，不卖出
                    else:
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
                
                # 重置减仓标志
                reduce_position_active = False
                last_reduce_position_price = 0
                
                # 重置上升阶段恢复观察标志
                uptrend_recovery_active = False
                uptrend_end_price = 0
                # 重置恢复买入标志
                is_recovery_buy = False
        
        # 情况4：上升阶段恢复买入（独立逻辑，不受position==0限制，可作为加仓逻辑）
        # 条件：在上升趋势中，处于恢复观察状态，价格从上升阶段结束时的价格上升3个点（固定数值）以上
        # 注意：此逻辑必须在减仓逻辑之后执行，以确保uptrend_recovery_active已被正确设置
        
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
                    # 恢复买入后，退出减仓模式
                    reduce_position_active = False
                    last_reduce_position_price = 0
                    # 标记为恢复买入持仓
                    is_recovery_buy = True
                    # 重置观察状态
                    observing_recovery_buy = False
                    recovery_buy_trigger_price = 0
        
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
            elif trade['action'] == '买回':
                log_print(f"  买回: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")
            elif trade['action'] == '减仓50%':
                log_print(f"  减仓50%: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            elif trade['action'] == '减仓清仓':
                log_print(f"  减仓清仓: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
            else:
                log_print(f"  卖出: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")
    
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
    run_backtest()
