# -*- coding: utf-8 -*-
"""
波动率策略 - 基于波动率变化的交易策略
买入：条件A(波动率连续向0靠近)、条件B(波动率从负变正)
卖出：波动率降低至前一天97%以下
"""


import pandas as pd
import numpy as np
import os
from ana_stocks import (
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    get_year_range
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

# 卖出策略全局参数
SELL_RATIO_THRESHOLD = 0.99999  # 波动率降至前一天97%以下全卖

# 买入条件全局参数
BUY_DECLINE_DAYS_REQUIRED = 3  # 波动率连续向0靠近所需天数（条件A）

# 主账户在卖出A与买入A之间的分批买入卖出开关
ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING = True  # 设置为True启用：主账户在卖出A与买入A之间分批买入卖出

# 主账户区间交易分批买入配置
ENABLE_MAIN_ACCOUNT_BUY_BY_ATR = True  # 是否启用基于价ATR倍数的买入（False则使用基于跌幅的买入）
# 基于跌幅的买入配置
MAIN_ACCOUNT_BUY_LEVELS = [-0.04, -0.08, -0.13]  # 买入触发跌幅（-4%, -8%, -13%）
# 基于价ATR倍数的买入配置
MAIN_ACCOUNT_BUY_ATR_LEVELS = [-2.0, -3.0, -4.0]  # 买入触发价ATR倍数阈值（对应买1: >-3, 买2: >-4, 买3: <=-4）
# 买入比例配置（两种方式共用）
MAIN_ACCOUNT_BUY_RATIOS = [0.20, 0.30, 0.40]      # 对应买入比例（20%, 30%, 50%）

# 主账户区间交易分批卖出配置
# 基于价ATR倍数的卖出配置（对应基于价ATR倍数的买入）
MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS = [0, 1.0, 2.0, 3.0]  # 卖出触发ATR倍数（0, 1.0, 2.0, 3.0）
# 基于补跌买入比例的卖出配置（对应基于跌幅的买入）
MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS = [0, 1.0, 2.0, 3.0]  # 卖出触发ATR倍数（0, 1.0, 2.0, 3.0）
# 卖出比例配置（两种方式共用）
MAIN_ACCOUNT_SELL_RATIOS = [0.30, 0.30, 0.25, 0.15]          # 对应卖出比例（30%, 30%, 40%）

# 主账户区间交易：剩余仓位小于等于该阈值时直接清仓，避免长期残仓
MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR = 300

# 爆发买入机制（卖出A与买入A之间）
ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY = True  # 是否启用爆发买入机制
MAIN_ACCOUNT_OUTBREAK_BUY_CONSECUTIVE_DAYS = 3  # 价ATR倍连续大于阈值的天数
MAIN_ACCOUNT_OUTBREAK_BUY_PRICE_ATR_THRESHOLD = 1.1  # 价ATR倍买入阈值
# 爆发卖出配置
MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS = 4  # 计算最高价的周期（默认5日，可设置为3日等）
MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD = 0.07  # 最高价下降超过该比例才卖出（防止小幅波动）
MAIN_ACCOUNT_OUTBREAK_SELL_DROP_THRESHOLD = -0.10  # 收盘价与N日最高价差距阈值，小于该值卖出（默认-10%）
MAIN_ACCOUNT_OUTBREAK_SELL_PREV_DAY_RATIO = 0.91  # 单日跌幅阈值，收盘价低于前一天该比例则卖出（默认0.96即跌幅4%）
MAIN_ACCOUNT_OUTBREAK_SELL_HOLDING_HIGH_THRESHOLD = 0.10  # 收盘价低于持仓期间最高价的阈值，超过该比例卖出（默认7%）

# 买入A延迟买入开关
ENABLE_BUY_A_DELAYED = True  # 是否启用延迟买入
# 新延迟买入规则参数
BUY_A_DELAYED_NEW_ENABLE = True  # 是否启用新的延迟买入规则
BUY_A_DELAYED_NEW_HIT_DAYS = 2  # 价ATR倍 < 5日价ATR倍累计天数
BUY_A_DELAYED_NEW_FIVE_DAY_ATR_THRESHOLD = -1.10  # 5日价ATR倍阈值
BUY_A_DELAYED_NEW_FORCE_BUY_ATR = -3.0  # 强制满仓的价ATR倍阈值




def calculate_slope_atr(df, ma_period=20, atr_period=14, n=5):
    """
    计算 MA20 的 ATR 归一化斜率（波动率）。
    公式: (MA20[t] - MA20[t-n]) / ATR[t]
    """
    # 计算 MA20
    df['MA20'] = df['收盘'].rolling(window=ma_period, min_periods=ma_period).mean()

    # 计算 ATR
    df['prev_close'] = df['收盘'].shift(1)
    df['tr1'] = df['最高'] - df['最低']
    df['tr2'] = (df['最高'] - df['prev_close']).abs()
    df['tr3'] = (df['最低'] - df['prev_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=atr_period, min_periods=atr_period).mean()

    # 计算 MA20 在 n 天内的变动
    df['MA20_shift'] = df['MA20'].shift(n)
    df['MA20_change'] = df['MA20'] - df['MA20_shift']

    # 计算归一化斜率
    df['波动率'] = df['MA20_change'] / df['ATR'].replace(0, np.nan)

    # 删除中间辅助列
    df.drop(columns=['prev_close', 'tr1', 'tr2', 'tr3', 'TR', 'MA20_shift', 'MA20_change'], inplace=True)

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
    df['atr14'] = df['tr'].rolling(window=14, min_periods=14).mean()
    
    # 计算波动率
    df = calculate_slope_atr(df, ma_period=20, atr_period=14, n=5)
    
    # 计算价格相对ATR的倍数：(收盘价 - MA20) / ATR
    df['价ATR倍'] = ((df['收盘'] - df['ma20']) / df['atr14']).replace([np.inf, -np.inf], np.nan)
    # 计算5日价ATR倍平均数
    df['5日价ATR平均'] = df['价ATR倍'].rolling(window=5, min_periods=1).mean()
    # 计算价ATR倍连续小于5日价ATR平均的天数
    df['连续小于5日ATR天数'] = 0
    current_streak = 0
    for i in range(len(df)):
        if i == 0:
            continue
        current_price_atr = df.loc[i, '价ATR倍']
        five_day_atr_avg = df.loc[i, '5日价ATR平均']
        if pd.notna(current_price_atr) and pd.notna(five_day_atr_avg) and current_price_atr < five_day_atr_avg:
            current_streak += 1
        else:
            current_streak = 0
        df.loc[i, '连续小于5日ATR天数'] = current_streak

    # 计算10日最低价及其ATR倍数（滚动窗口方式）
    # 对于每一天，找到最近10天内的最低价，并记录那一天的价ATR倍
    df['10日最低价ATR倍数'] = np.nan
    
    # 计算N日收盘价最高列（用于爆发卖出，周期可配置）
    df[f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'] = df['收盘'].rolling(window=MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS, min_periods=1).max()
    
    for i in range(len(df)):
        # 获取最近10天的窗口（包括当天）
        window_start = max(0, i - 9)
        window_end = i + 1
        
        # 在窗口内找到最低价的索引
        window_prices = df.loc[window_start:window_end-1, '收盘']
        window_price_atr = df.loc[window_start:window_end-1, '价ATR倍']
        
        if len(window_prices) > 0:
            min_price_idx = window_prices.idxmin()
            # 记录最低价那一天的价ATR倍
            price_atr_at_min = df.loc[min_price_idx, '价ATR倍']
            df.loc[i, '10日最低价ATR倍数'] = price_atr_at_min
    
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
    
    # 执行回测逻辑
    return _run_backtest_core(stock_code, df)


def run_backtest_with_data(stock_code: str, stock_data: pd.DataFrame):
    """使用预加载的数据进行回测（用于批量回测优化）"""
    if stock_data is None or len(stock_data) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(stock_data) if stock_data is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(stock_data)
    
    # 执行回测逻辑
    return _run_backtest_core(stock_code, df)


def _run_backtest_core(stock_code: str, df: pd.DataFrame):
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 初始化交易变量
    initial_capital = 100000
    cash = initial_capital
    position = 0
    buy_price = 0
    trades = []
    trade_count = 0
    
    # 符合A买入条件的统计
    total_condition_a_count = 0  # 所有符合A买入条件的次数（无论是否持仓）
    actual_condition_a_buy_count = 0  # 实际执行A买入的次数
    
    # 买入条件计数器
    volatility_declining_days = 0  # 波动率连续向0靠近天数（数值变大）
    prev_volatility = None         # 前一天波动率
    
    # 持仓开始日期（用于计算持仓天数）
    holding_start_date = None

    # 买入A延迟买入状态变量
    buy_a_delayed_pending = False  # 是否有待买A
    buy_a_pending_price = 0.0  # 待买A标记价格
    buy_a_below_ma20_atr_hit_count = 0  # 待买A期间，L2累计命中次数
    buy_a_marked = False  # 是否已经标记了待买A
    
    # 主账户在卖出A与买入A之间的分批买入卖出状态变量
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        # 根据开关选择买入触发方式对应的数组长度
        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)  # 主账户卖出A与买入A之间的买入档位
        else:
            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)  # 主账户卖出A与买入A之间的买入档位
        main_account_sell_buy_position = 0  # 主账户在卖出A与买入A之间的持仓
        main_account_sell_buy_price = 0  # 主账户在卖出A与买入A之间的加权平均买入价格
        # 根据买入方式选择对应的卖出配置长度
        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        else:
            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        main_account_drop_anchor_price = 0
        main_account_initial_cash = 0  # 主账户区间交易的初始资金（卖出时的现金）
        # 爆发买入机制状态变量
        main_account_outbreak_buy_consecutive_days = 0  # 波动率连续大于阈值天数
        main_account_outbreak_buy_active = False  # 是否处于爆发买入持仓状态
        main_account_outbreak_buy_price = 0  # 爆发买入价格
        main_account_outbreak_sell_high = 0  # 爆发买入后的N日最高价
        # 持仓期间最高价（适用于所有持仓类型：主仓、区间交易、爆发买入等）
        main_account_holding_high = 0
    else:
        main_account_sell_buy_levels_triggered = []
        main_account_sell_buy_position = 0
        main_account_sell_buy_price = 0
        main_account_sell_sell_levels_triggered = []
        main_account_drop_anchor_price = 0
        main_account_initial_cash = 0
        # 爆发买入机制状态变量
        main_account_outbreak_buy_consecutive_days = 0
        main_account_outbreak_buy_active = False
        main_account_outbreak_buy_price = 0
        main_account_outbreak_sell_high = 0  # 爆发买入后的N日最高价
        # 持仓期间最高价（适用于所有持仓类型：主仓、区间交易、爆发买入等）
        main_account_holding_high = 0

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
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"买入条件A: (连续向0靠近{BUY_DECLINE_DAYS_REQUIRED}天且波动率<0) - 全仓买入")
    log_print(f"卖出条件: 波动率>0且降低时，降至前一天{SELL_RATIO_THRESHOLD*100:.0f}%以下则全卖")
    log_print(f"{'='*165}\n")

    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'ATR14':>8} {'波动率':>8} {'价ATR倍':>8} {'5日ATR平均':>8} {'10日最低价ATR倍数':>16} {f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高':>8} {'持仓最高':>8} {'连续天数':>8}   {'操作':<30} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * 165)
    
    # 遍历每一天进行回测
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        ma20 = row['ma20']
        volatility = row['波动率']
        
        action = ""
        condition_a = False  # 初始化条件A标记
        
        # 更新持仓期间最高价（只要有持仓就更新）
        total_position = position + main_account_sell_buy_position
        if total_position > 0:
            if main_account_holding_high == 0:
                # 首次持仓，初始化最高价
                main_account_holding_high = close_price
            elif close_price > main_account_holding_high:
                # 价格创新高，更新最高价
                main_account_holding_high = close_price
        
        # 确保数据有效
        if pd.notna(volatility):
            # 判断波动率变化（在更新prev_volatility之前判断）
            is_volatility_declining = False
            is_volatility_increasing_toward_zero = False  # 波动率向0靠近（数值变大）
            if prev_volatility is not None:
                if volatility > prev_volatility:
                    # 判断是否在向0靠近（当前和前一期都为负，且当前更大/更接近0）
                    if volatility < 0 and prev_volatility < 0:
                        is_volatility_increasing_toward_zero = True
                elif volatility < prev_volatility:
                    is_volatility_declining = True
            
            # 更新波动率在阈值以上的连续天数（必须在卖出判断之前更新）

            # 卖出策略
            should_sell = False
            sell_reason = ""

            # 原始卖出A信号：波动率>0且降低，且降至前一天阈值以下
            sell_a_signal_triggered = False
            if volatility > 0 and is_volatility_declining:
                volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                if volatility_ratio <= SELL_RATIO_THRESHOLD:
                    sell_a_signal_triggered = True
            
            if sell_a_signal_triggered and position > 0:
                # 未启用延迟卖出，立即全仓卖出
                should_sell = True
                sell_reason = "比率卖出"
            
            # 卖出逻辑
            if position > 0:
                if should_sell:
                    # 立即卖出（全仓）
                    sell_price = close_price
                    sell_value = position * sell_price
                    profit = (sell_price - buy_price) * position
                    cash += sell_value
                    action = f"卖出@{sell_price:.2f}({sell_reason})"
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '卖出',
                        'price': sell_price,
                        'shares': position,
                        'profit': profit
                    })
                    
                    position = 0
                    buy_price = 0
                    # 卖出后重置计数器
                    volatility_declining_days = 0
                    # 重置持仓开始日期
                    holding_start_date = None
                    # 重置买入A延迟买入状态
                    buy_a_delayed_pending = False
                    buy_a_marked = False
                    buy_a_pending_price = 0.0
                    buy_a_below_ma20_atr_hit_count = 0

                    # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                        # 设置锚定价格为卖出价格
                        main_account_drop_anchor_price = sell_price
                        # 记录卖出时的现金作为区间交易的初始资金
                        main_account_initial_cash = cash
                        # 重置主账户在卖出A与买入A之间的分批买入卖出状态变量
                        # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        # 根据买入方式选择对应的卖出配置长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        else:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
            # 更新前一天的波动率
            prev_volatility = volatility

            # 买入条件判断
            # 条件：波动率为负，连续向0靠近（数值变大）
            if is_volatility_increasing_toward_zero:
                volatility_declining_days += 1
            else:
                volatility_declining_days = 0

            # 条件A：连续向0靠近指定天数，且当天波动率<0（负值区间）
            condition_a = (volatility_declining_days >= BUY_DECLINE_DAYS_REQUIRED and
                           volatility < 0)
            
            # 统计所有符合A买入条件的次数
            if condition_a:
                total_condition_a_count += 1
            # 买入逻辑：
            # 1) ENABLE_BUY_A_DELAYED=False -> 直接买入A
            # 2) ENABLE_BUY_A_DELAYED=True  -> 先待买A标记，待触发后再真正买入A（待买期间不做任何区间操作）
            if position == 0:
                if condition_a and (not buy_a_delayed_pending):
                    if ENABLE_BUY_A_DELAYED:
                        buy_a_delayed_pending = True
                        buy_a_marked = True
                        buy_a_pending_price = close_price
                        buy_a_below_ma20_atr_hit_count = 0
                        action = f"待买A@{close_price:.2f}(基准价标记)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '待买A',
                            'price': close_price,
                            'shares': 0
                        })
                        volatility_declining_days = 0
                    else:
                        buy_price = close_price
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and main_account_sell_buy_position > 0:
                            sell_value = main_account_sell_buy_position * buy_price
                            sell_profit = (buy_price - main_account_sell_buy_price) * main_account_sell_buy_position
                            cash += sell_value
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '卖出',
                                'price': buy_price,
                                'shares': main_account_sell_buy_position,
                                'profit': sell_profit
                            })
                            trade_count += 1
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            # 根据买入方式选择对应的卖出配置长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        else:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                        new_position = int(cash / buy_price / 100) * 100
                        if new_position >= 100:
                            position = new_position
                            cash -= position * buy_price
                            trade_count += 1
                            actual_condition_a_buy_count += 1
                            action = f"买入A@{buy_price:.2f}"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'shares': position
                            })
                            if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                                main_account_initial_cash = cash
                                main_account_drop_anchor_price = buy_price
                                # 根据开关选择买入触发方式对应的数组长度
                                if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                                    main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                                else:
                                    main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                                # 根据买入方式选择对应的卖出配置长度
                                if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                                    main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                                else:
                                    main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                                main_account_sell_buy_position = 0
                                main_account_sell_buy_price = 0
                            volatility_declining_days = 0
                            if holding_start_date is None:
                                holding_start_date = date_str

            # 延迟买入执行：仅当允许买入时，才从待买A切换到真正买入A
            if ENABLE_BUY_A_DELAYED and position == 0 and buy_a_delayed_pending:
                price_atr_multiplier = 0
                if pd.notna(ma20) and ma20 > 0 and pd.notna(df.loc[i, 'ATR']) and df.loc[i, 'ATR'] > 0:
                    price_atr_multiplier = (close_price - ma20) / df.loc[i, 'ATR']
                can_buy = False
                # 待买A期间如果触发原始卖出A信号，则本轮待买失效并重置，避免跨越原有买卖A周期
                if should_sell:
                    buy_a_delayed_pending = False
                    buy_a_marked = False
                    buy_a_pending_price = 0.0
                    buy_a_below_ma20_atr_hit_count = 0
                    action = f"待买A失效@{close_price:.2f}(到达卖出A重置)"
                
                # 新延迟买入规则
                if BUY_A_DELAYED_NEW_ENABLE:
                    # 1. 价ATR倍数 > 0 直接买入
                    if price_atr_multiplier > 0:
                        can_buy = True
                        buy_a_below_ma20_atr_hit_count = 0
                    # 2. 中途遇到价ATR倍数 < -3的情况直接满仓
                    elif price_atr_multiplier < BUY_A_DELAYED_NEW_FORCE_BUY_ATR:
                        can_buy = True
                        buy_a_below_ma20_atr_hit_count = 0
                    else:
                        # 获取5日价ATR平均
                        five_day_atr_avg = df.loc[i, '5日价ATR平均'] if pd.notna(df.loc[i, '5日价ATR平均']) else -999
                        # 3. 价ATR倍数 < 5日价ATR倍数 累计到3天
                        # 且当天的5日价ATR倍数需要大于设定的值默认是 -1.2
                        # 从待买的第二天开始算第一天
                        if buy_a_delayed_pending and not buy_a_marked:
                            if price_atr_multiplier < five_day_atr_avg and five_day_atr_avg > BUY_A_DELAYED_NEW_FIVE_DAY_ATR_THRESHOLD:
                                buy_a_below_ma20_atr_hit_count += 1
                                if buy_a_below_ma20_atr_hit_count >= BUY_A_DELAYED_NEW_HIT_DAYS:
                                    can_buy = True
                                    buy_a_below_ma20_atr_hit_count = 0
                            else:
                                buy_a_below_ma20_atr_hit_count = 0
                        # 待买标记当天不计算天数
                        elif buy_a_marked:
                            buy_a_marked = False
                

                if can_buy:
                    buy_price = close_price
                    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and main_account_sell_buy_position > 0:
                        sell_value = main_account_sell_buy_position * buy_price
                        sell_profit = (buy_price - main_account_sell_buy_price) * main_account_sell_buy_position
                        cash += sell_value
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '卖出',
                            'price': buy_price,
                            'shares': main_account_sell_buy_position,
                            'profit': sell_profit
                        })
                        trade_count += 1
                        main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        # 根据买入方式选择对应的卖出配置长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        else:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                    new_position = int(cash / buy_price / 100) * 100
                    if new_position >= 100:
                        position = new_position
                        cash -= position * buy_price
                        trade_count += 1
                        actual_condition_a_buy_count += 1
                        action = f"买入A@{buy_price:.2f}(由待买A触发)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'shares': position
                        })
                        buy_a_delayed_pending = False
                        buy_a_marked = False
                        buy_a_pending_price = 0.0
                        buy_a_below_ma20_atr_hit_count = 0
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                            main_account_initial_cash = cash
                            main_account_drop_anchor_price = buy_price
                            # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            # 根据买入方式选择对应的卖出配置长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        else:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                        main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        volatility_declining_days = 0
                        if holding_start_date is None:
                            holding_start_date = date_str

        # ========================================
        # 爆发买入机制：优先级最高，可在任何情况下触发
        # 触发条件：价ATR倍连续大于阈值时买入（无论是否有持仓、是否在区间交易中）
        # ========================================
        if ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY and not main_account_outbreak_buy_active:
            current_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
            if current_price_atr > MAIN_ACCOUNT_OUTBREAK_BUY_PRICE_ATR_THRESHOLD:
                main_account_outbreak_buy_consecutive_days += 1
                # 达到连续天数要求才买入
                if main_account_outbreak_buy_consecutive_days >= MAIN_ACCOUNT_OUTBREAK_BUY_CONSECUTIVE_DAYS:
                    # 全仓买入（使用所有可用资金）
                    new_position = int(cash / close_price / 100) * 100
                    if new_position >= 100 and cash >= new_position * close_price:
                        cost = new_position * close_price
                        cash -= cost
                        # 如果之前有持仓，更新加权平均价格
                        if main_account_sell_buy_position > 0:
                            main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
                        else:
                            main_account_sell_buy_price = close_price
                        main_account_sell_buy_position += new_position
                        main_account_outbreak_buy_price = close_price
                        main_account_outbreak_buy_active = True
                        # 初始化N日最高价为当前N日最高
                        high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                        main_account_outbreak_sell_high = row[high_col] if pd.notna(row[high_col]) else close_price
                        # 初始化持仓期间最高价为买入价格
                        main_account_holding_high = close_price
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': close_price,
                            'shares': new_position,
                            'type': '爆发买入'
                        })
                        action = f"主账户爆发买入@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                        # 记录持仓开始日期
                        if holding_start_date is None:
                            holding_start_date = date_str
                        # 买入后重置计数器
                        main_account_outbreak_buy_consecutive_days = 0
            else:
                # 不满足条件，重置计数器
                main_account_outbreak_buy_consecutive_days = 0

        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and position == 0 and (not buy_a_delayed_pending) and main_account_drop_anchor_price > 0:
            drop_anchor_price = main_account_drop_anchor_price
            price_drop_pct = (close_price - drop_anchor_price) / drop_anchor_price if drop_anchor_price > 0 else 0
            
            # 标记当天是否有买入操作
            has_buy_today = False

            if close_price < ma20 and drop_anchor_price > 0 and not has_buy_today:
                executed_drop_levels = []
                
                # 根据开关选择买入触发方式
                if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                    # 基于价ATR倍数的买入
                    # 计算价ATR倍数
                    price_atr_multiplier = 0
                    if pd.notna(ma20) and ma20 > 0 and pd.notna(df.loc[i, 'ATR']) and df.loc[i, 'ATR'] > 0:
                        price_atr_multiplier = (close_price - ma20) / df.loc[i, 'ATR']
                    
                    for drop_idx, level in enumerate(MAIN_ACCOUNT_BUY_ATR_LEVELS):
                        if main_account_sell_buy_levels_triggered[drop_idx]:
                            continue
                        # 基于价ATR倍数的触发条件（互斥区间）
                        # 买1: -3 < 价ATR倍 <= -2
                        # 买2: -4 < 价ATR倍 <= -3
                        # 买3: 价ATR倍 <= -4
                        if drop_idx == 0 and (MAIN_ACCOUNT_BUY_ATR_LEVELS[1] < price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[0]):
                            pass
                        elif drop_idx == 1 and (MAIN_ACCOUNT_BUY_ATR_LEVELS[2] < price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[1]):
                            pass
                        elif drop_idx == 2 and price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[2]:
                            pass
                        else:
                            continue

                        ratio = MAIN_ACCOUNT_BUY_RATIOS[drop_idx]
                        buy_amount = main_account_initial_cash * ratio
                        buy_amount = min(buy_amount, cash)
                        new_position = int(buy_amount / close_price / 100) * 100
                        if new_position < 100 or cash < new_position * close_price:
                            continue

                        cost = new_position * close_price
                        cash -= cost
                        # 更新总持仓的加权平均价格
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = close_price
                        else:
                            main_account_sell_buy_price = (
                                main_account_sell_buy_price * main_account_sell_buy_position
                                + close_price * new_position
                            ) / (main_account_sell_buy_position + new_position)
                        main_account_sell_buy_position += new_position
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': close_price,
                            'shares': new_position,
                            'level': drop_idx + 1
                        })
                        main_account_sell_buy_levels_triggered[drop_idx] = True
                        executed_drop_levels.append(drop_idx)
                        # 记录持仓开始日期（区间交易首次买入）
                        if holding_start_date is None:
                            holding_start_date = date_str
                else:
                    # 基于跌幅的买入
                    for drop_idx, level in enumerate(MAIN_ACCOUNT_BUY_LEVELS):
                        if main_account_sell_buy_levels_triggered[drop_idx]:
                            continue
                        if price_drop_pct > level:
                            continue

                        ratio = MAIN_ACCOUNT_BUY_RATIOS[drop_idx]
                        buy_amount = main_account_initial_cash * ratio
                        buy_amount = min(buy_amount, cash)
                        new_position = int(buy_amount / close_price / 100) * 100
                        if new_position < 100 or cash < new_position * close_price:
                            continue

                        cost = new_position * close_price
                        cash -= cost
                        # 更新总持仓的加权平均价格
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = close_price
                        else:
                            main_account_sell_buy_price = (
                                main_account_sell_buy_price * main_account_sell_buy_position
                                + close_price * new_position
                            ) / (main_account_sell_buy_position + new_position)

                        main_account_sell_buy_position += new_position
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': close_price,
                            'shares': new_position,
                            'level': drop_idx + 1
                        })
                        main_account_sell_buy_levels_triggered[drop_idx] = True
                        executed_drop_levels.append(drop_idx)
                        # 记录持仓开始日期（区间交易首次买入）
                        if holding_start_date is None:
                            holding_start_date = date_str

                if executed_drop_levels:
                    drop_levels_str = ','.join([f"买{idx + 1}" for idx in executed_drop_levels])
                    action = f"主账户{drop_levels_str}@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                    has_buy_today = True
                    # 重置卖出档位标记
                    if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                    else:
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)

            # 只有当当天没有买入操作时，才执行卖出
            if main_account_sell_buy_position > 0 and main_account_sell_buy_price > 0 and not has_buy_today:
                # 检查是否触发爆发卖出
                stop_loss_triggered = False
                
                # 爆发买入锁定：当爆发买入激活时，只检查爆发卖出条件，跳过其他卖出机制
                if main_account_outbreak_buy_active:
                    # 检查爆发买入卖出条件（三条件机制，谁先到先卖出）
                    high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                    current_high = row[high_col] if pd.notna(row[high_col]) else close_price
                    
                    # 条件A：N日最高价没有创新高（即当前最高价 <= 记录的最高价）
                    # 条件B：收盘价与N日最高价的差距超过阈值
                    # 条件C：价格低于前一天的设定阈值（单日大幅下跌）
                    # 条件D：收盘价低于持仓期间最高价的特定阈值

                    if current_high > main_account_outbreak_sell_high:
                        # 创新高，更新N日最高价
                        main_account_outbreak_sell_high = current_high

                    # 计算四个条件（只要之前有过最高价记录就可以计算）
                    outbreak_sell_reason = ''  # 记录触发条件
                    if main_account_outbreak_sell_high > 0:
                        # 条件A：N日最高价下降比例（相对于历史最高）
                        high_drop_pct = (main_account_outbreak_sell_high - current_high) / main_account_outbreak_sell_high
                        condition_a = high_drop_pct > MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD

                        # 条件B：收盘价与N日最高价的差距
                        price_drop_pct = (close_price - current_high) / current_high if current_high > 0 else 0
                        condition_b = price_drop_pct < MAIN_ACCOUNT_OUTBREAK_SELL_DROP_THRESHOLD

                        # 条件C：单日跌幅超过阈值（收盘价 < 前一天收盘价 * 阈值）
                        prev_close = df.iloc[i-1]['收盘'] if i > 0 else close_price
                        condition_c = close_price < prev_close * MAIN_ACCOUNT_OUTBREAK_SELL_PREV_DAY_RATIO

                        # 条件D：收盘价低于持仓期间最高价的特定阈值
                        # 更新持仓期间最高价
                        if close_price > main_account_holding_high:
                            main_account_holding_high = close_price
                        # 计算收盘价与持仓期间最高价的差距比例
                        holding_high_drop_pct = (main_account_holding_high - close_price) / main_account_holding_high if main_account_holding_high > 0 else 0
                        condition_d = holding_high_drop_pct > MAIN_ACCOUNT_OUTBREAK_SELL_HOLDING_HIGH_THRESHOLD

                        # 任一条件满足即触发卖出，并记录触发条件
                        if condition_a or condition_b or condition_c or condition_d:
                            stop_loss_triggered = True
                            # 记录触发的条件（可能有多个）
                            reasons = []
                            if condition_a:
                                reasons.append('A')
                            if condition_b:
                                reasons.append('B')
                            if condition_c:
                                reasons.append('C')
                            if condition_d:
                                reasons.append('D')
                            outbreak_sell_reason = '+'.join(reasons)

                current_atr = row['atr14'] if pd.notna(row['atr14']) else 0
                # 爆发买入锁定：当爆发买入激活时，跳过正常的档位卖出逻辑
                can_evaluate_sell = stop_loss_triggered or (not main_account_outbreak_buy_active)

                if can_evaluate_sell:
                    # 计算基于MA20的价ATR倍数
                    ma20_price_atr_multiplier = 0
                    if pd.notna(ma20) and ma20 > 0 and current_atr > 0:
                        ma20_price_atr_multiplier = (close_price - ma20) / current_atr

                    price_atr_multiplier = (close_price - main_account_sell_buy_price) / current_atr if current_atr > 0 else 0

                    # 根据买入方式选择对应的卖出配置
                    if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                        sell_levels = MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS
                        sell_trigger_value = ma20_price_atr_multiplier
                    else:
                        sell_levels = MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS
                        sell_trigger_value = price_atr_multiplier
                    sell_ratios = MAIN_ACCOUNT_SELL_RATIOS

                    max_triggered_level = -1
                    newly_triggered_levels = []
                    for sell_idx in range(len(sell_levels) - 1, -1, -1):
                        if sell_trigger_value >= sell_levels[sell_idx]:
                            max_triggered_level = sell_idx
                            break

                    if (not stop_loss_triggered) and max_triggered_level >= 0:
                        # 追跌买入的卖出档位标记
                        trigger_cap = min(max_triggered_level + 1, len(main_account_sell_sell_levels_triggered))
                        for idx in range(trigger_cap):
                            if not main_account_sell_sell_levels_triggered[idx]:
                                main_account_sell_sell_levels_triggered[idx] = True
                                newly_triggered_levels.append(idx)

                    trade_level = 0
                    # reset rise/drop independent prices
                    if stop_loss_triggered:
                        # 爆发买入卖出所有仓位
                        sell_shares = main_account_sell_buy_position
                        sell_shares = max(sell_shares, 0)
                    elif newly_triggered_levels:
                        step_ratio = sum(sell_ratios[idx] for idx in newly_triggered_levels if idx < len(sell_ratios))
                        sell_shares = int(main_account_sell_buy_position * step_ratio)
                        sell_shares = min(sell_shares, main_account_sell_buy_position)
                        trade_level = newly_triggered_levels[-1] + 1
                    elif max_triggered_level >= 0:
                        # 这种情况应该不会发生，因为已经在上面处理了newly_triggered_levels
                        total_ratio = sum(sell_ratios[:max_triggered_level + 1])
                        sell_shares = int(main_account_sell_buy_position * total_ratio)
                        sell_shares = min(sell_shares, main_account_sell_buy_position)
                        trade_level = max_triggered_level + 1
                    else:
                        sell_shares = 0
                    if sell_shares > 0:
                        remaining_after_sell = main_account_sell_buy_position - sell_shares
                        if remaining_after_sell <= MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR:
                            sell_shares = main_account_sell_buy_position
                        sell_value = sell_shares * close_price
                        profit = (close_price - main_account_sell_buy_price) * sell_shares
                        cash += sell_value
                        main_account_sell_buy_position -= sell_shares
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '卖出',
                            'price': close_price,
                            'shares': sell_shares,
                            'profit': profit,
                            'level': trade_level if not stop_loss_triggered else 0
                        })
                        if stop_loss_triggered:
                            triggered_levels = f"爆发卖出({outbreak_sell_reason})"
                        else:
                            if newly_triggered_levels:
                                triggered_levels = ",".join([f"追跌卖{i+1}" for i in newly_triggered_levels])
                            else:
                                triggered_levels = ",".join([f"追跌卖{i+1}" for i in range(max_triggered_level + 1)])
                        remaining_position = main_account_sell_buy_position
                        action = f"主账户{triggered_levels}@{close_price:.2f} 持仓{remaining_position}"
                        # 卖出后重置买入档位，允许在更低价格继续追跌买入
                        # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = 0
                            # 根据开关选择买入触发方式对应的数组长度
                            if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                                main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                            else:
                                main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            # 根据买入方式选择对应的卖出配置长度
                            if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                                main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                            else:
                                main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                            # reset rise/drop independent prices
                            holding_start_date = None
                        # 重置爆发买入状态
                        main_account_outbreak_buy_active = False
                        main_account_outbreak_buy_price = 0
                        main_account_outbreak_buy_consecutive_days = 0
                        main_account_outbreak_sell_high = 0  # 重置N日最高价

        # 如果持仓全部卖出，重置持仓最高价
        total_position = position + main_account_sell_buy_position
        if total_position == 0:
            main_account_holding_high = 0

        display_position = position
        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
            display_position += main_account_sell_buy_position
        market_value = cash + display_position * close_price if display_position > 0 else cash
        position_str = f"{display_position}" if display_position > 0 else "0"
        
        # 数据显示（所有情况都显示，包括可买未买）
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        atr14_str = f"{row['atr14']:.2f}" if pd.notna(row['atr14']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        price_atr_ratio_str = f"{row['价ATR倍']:.2f}" if pd.notna(row['价ATR倍']) else "N/A"
        # 计算5日ATR平均
        five_day_atr_avg_str = f"{row['5日价ATR平均']:.2f}" if pd.notna(row['5日价ATR平均']) else "N/A"
        # 连续小于5日ATR天数
        consecutive_days_str = f"{row['连续小于5日ATR天数']:.0f}" if pd.notna(row['连续小于5日ATR天数']) else "0"
        # 10日最低ATR倍数
        ten_day_low_atr_str = f"{row['10日最低价ATR倍数']:.2f}" if pd.notna(row['10日最低价ATR倍数']) else "N/A"
        # N日最高价（可配置周期）
        high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
        n_day_high_str = f"{row[high_col]:.2f}" if pd.notna(row[high_col]) else "N/A"
        # 持仓期间最高价（适用于所有持仓）
        total_position = position + main_account_sell_buy_position
        holding_high_str = f"{main_account_holding_high:.2f}" if total_position > 0 and main_account_holding_high > 0 else "N/A"

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {atr14_str:>8} {volatility_str:>8} {price_atr_ratio_str:>8} {five_day_atr_avg_str:>8} {ten_day_low_atr_str:>12} {n_day_high_str:>8} {holding_high_str:>8} {consecutive_days_str:>8}   {action:<30} {position_str:>8} {market_value:>12,.2f}")
    
    # 计算最终收益（主仓 + 主账户区间仓位）
    final_position = position
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        final_position += main_account_sell_buy_position
    final_value = cash + final_position * df.iloc[-1]['收盘'] if final_position > 0 else cash
    final_profit = final_value - initial_capital
    
    # 收集持仓信息
    holding_info = None
    if final_position > 0:
        last_close = df.iloc[-1]['收盘']
        last_date = df.iloc[-1]['date']
        # 计算持仓成本
        if position > 0 and buy_price > 0:
            # 有主仓持仓
            holding_cost = buy_price
        elif ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and main_account_sell_buy_position > 0:
            # 只有区间交易持仓
            holding_cost = main_account_sell_buy_price
        else:
            holding_cost = 0
        
        if holding_cost > 0:
            price_change_pct = (last_close - holding_cost) / holding_cost * 100
            # 计算持仓天数
            holding_days = 0
            if holding_start_date:
                from datetime import datetime
                # 处理日期字符串，只取日期部分
                start_str = str(holding_start_date)[:10]
                end_str = str(last_date)[:10]
                start_dt = datetime.strptime(start_str, '%Y-%m-%d')
                end_dt = datetime.strptime(end_str, '%Y-%m-%d')
                holding_days = (end_dt - start_dt).days
            holding_info = {
                'position': final_position,
                'cost_price': holding_cost,
                'current_price': last_close,
                'price_change_pct': price_change_pct,
                'start_date': holding_start_date,
                'holding_days': holding_days
            }
    
    log_print(f"\n{'='*175}")
    log_print(f"回测结果统计")
    log_print(f"{'='*175}")
    log_print(f"买卖次数: {trade_count}")
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"最终资金: {final_value:,.2f}")
    log_print(f"总盈利: {final_profit:,.2f}")
    log_print(f"收益率: {(final_profit/initial_capital)*100:.2f}%")
    log_print(f"\n条件A买入统计:")
    log_print(f"  符合A买入条件次数: {total_condition_a_count}")
    log_print(f"  实际执行A买入次数: {actual_condition_a_buy_count}")
    
    if trades:
        log_print(f"\n交易明细:")
        log_print(f"{'序号':<6} {'日期':<6} {'操作':<6} {'价格':>10} {'股数':>10} {'盈亏':>12} {'盈亏%':>8}")
        log_print("-" * 80)
        for idx, trade in enumerate(trades, 1):
            profit_str = f"{trade.get('profit', 0):,.2f}" if 'profit' in trade else "-"
            # 计算盈亏百分比
            if 'profit' in trade and trade['action'] == '卖出':
                # 找到对应的买入交易
                buy_trade = None
                for t in trades[:idx-1]:
                    if t['action'] == '买入':
                        buy_trade = t
                if buy_trade:
                    profit_pct = (trade.get('profit', 0) / (buy_trade['price'] * buy_trade['shares'])) * 100
                    profit_pct_str = f"{profit_pct:+.2f}%"
                else:
                    profit_pct_str = "-"
            else:
                profit_pct_str = "-"
            log_print(f"{idx:<6} {trade['date']:<12} {trade['action']:<6} {trade['price']:>10.2f} {trade['shares']:>10} {profit_str:>12} {profit_pct_str:>8}")

    log_print(f"{'='*175}")

    # 预测第二天卖出触发价格（只在有持仓时计算）
    if position > 0:
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        last_ma20 = last_row['ma20']
        last_atr14 = last_row['atr14']
        last_volatility = last_row['波动率']
        
        log_print(f"\n【第二天卖出价格预测 - 基于当前价格{last_close:.2f}】")
        log_print(f"预测日期: {df.iloc[-1]['date'].strftime('%Y-%m-%d') if hasattr(df.iloc[-1]['date'], 'strftime') else str(df.iloc[-1]['date'])[:10]}")
        log_print(f"当前持仓: {position}股")
        
        # 收集所有可能的卖出触发价格
        sell_triggers = []
        
        # 波动率比率卖出
        if pd.notna(last_volatility) and pd.notna(last_atr14) and last_atr14 > 0 and len(df) >= 5:
            ma20_t_minus_4 = df.iloc[-5]['ma20'] if pd.notna(df.iloc[-5]['ma20']) else last_ma20
            
            # 计算波动率比率卖出的触发价格
            # 如果波动率 > 0 且下一天波动率降低至 SELL_RATIO_THRESHOLD 以下
            if last_volatility > 0:
                target_volatility = last_volatility * SELL_RATIO_THRESHOLD
                target_ma20_change = target_volatility * last_atr14
                target_price_vol = (target_ma20_change + ma20_t_minus_4) * 20 - last_ma20 * 19
                
                sell_triggers.append({
                    'name': '波动率比率卖出',
                    'price': target_price_vol,
                    'condition': f'波动率降至{SELL_RATIO_THRESHOLD*100:.0f}%以下',
                    'priority': 5
                })
        
        # 按价格从高到低排序，找出最严格的触发条件
        # 用户想知道：价格低于多少会触发卖出
        valid_triggers = [t for t in sell_triggers if pd.notna(t['price']) and t['price'] > 0]
        
        if valid_triggers:
            # 按价格排序（从高到低）
            valid_triggers.sort(key=lambda x: x['price'], reverse=True)
            
            log_print(f"\n预测卖出触发条件（按触发价格从高到低）：")
            for i, trigger in enumerate(valid_triggers, 1):
                change_pct = (trigger['price'] - last_close) / last_close * 100
                log_print(f"  {i}. {trigger['name']}: {trigger['condition']} ({change_pct:+.2f}%)")
            
            # 找出最严格的触发条件（最高的触发价格）
            strictest_trigger = valid_triggers[0]
            log_print(f"\n【结论】")
            log_print(f"  最严格触发条件: {strictest_trigger['name']}")
            log_print(f"  触发价格: {strictest_trigger['price']:.2f}")
            change_pct = (strictest_trigger['price'] - last_close) / last_close * 100
            log_print(f"  价格变动: {change_pct:+.2f}%")
            log_print(f"  说明: 如果下一天收盘价 {strictest_trigger['condition']}，将触发卖出")
            
            # 显示其他可能的触发条件
            if len(valid_triggers) > 1:
                log_print(f"\n  其他可能的触发条件:")
                for trigger in valid_triggers[1:3]:  # 只显示前2个
                    change_pct = (trigger['price'] - last_close) / last_close * 100
                    log_print(f"    - {trigger['name']}: {trigger['price']:.2f} ({change_pct:+.2f}%)")
        else:
            log_print(f"\n  根据当前条件，暂时无法预测明确的卖出触发价格")
        
        log_print(f"{'='*175}")
    # 写入文件前做中文动作文本校验，避免乱码占位符混入输出
    invalid_action_tokens = ['??', '主账户?']
    for trade in trades:
        trade_action = trade.get('action', '')
        if any(token in trade_action for token in invalid_action_tokens):
            raise ValueError(f"检测到异常动作文本: {trade_action}")

    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    # 计算总收益率和年度收益率
    total_return = (final_profit / initial_capital) * 100 if initial_capital > 0 else 0
    
    yearly_returns = {}
    if trades:
        for trade in trades:
            if 'profit' in trade and 'date' in trade:
                year = int(trade['date'][:4])
                if year not in yearly_returns:
                    yearly_returns[year] = 0
                yearly_returns[year] += trade['profit']
    
    for year in yearly_returns:
        yearly_returns[year] = (yearly_returns[year] / initial_capital) * 100
    
    return total_return, yearly_returns, {'trades': trades, 'final_value': final_value, 'holding_info': holding_info}


if __name__ == "__main__":
    run_backtest()


