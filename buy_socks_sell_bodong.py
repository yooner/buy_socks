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

# 卖出A延迟卖出开关
ENABLE_SELL_A_DELAYED = True  # 是否启用卖出A延迟卖出
SELL_A_DELAYED_RATIO_THRESHOLD = 0.25  # 延迟卖出时每次卖出的初始仓位比例
SELL_A_PRICE_ATR_MA_DAYS = 5  # 价ATR倍数移动平均线天数

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
MAIN_ACCOUNT_OUTBREAK_BUY_PRICE_ATR_THRESHOLD = 1.5  # 价ATR倍买入阈值
# 爆发卖出配置
MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS = 5  # 计算最高价的周期（默认3日，可设置为5日等）
MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD = 0.07  # 最高价下降超过该比例才卖出（防止小幅波动）

# 主账户区间交易：上涨场景分批买入（参考outbreak的趋势确认思路）
ENABLE_MAIN_ACCOUNT_UPTREND_BUY = True
MAIN_ACCOUNT_UPTREND_LEVELS = [0.03, 0.06, 0.10]   # 相对锚定价上涨3%/6%/10%触发
MAIN_ACCOUNT_UPTREND_RATIOS = [0.40, 0.40, 0.20]   # 对应分批投入比例
MAIN_ACCOUNT_UPTREND_REQUIRE_MA20_UP = False         # 要求MA20较前一日上行
# 追涨过滤（防止“只是上涨一点就追”）
MAIN_ACCOUNT_UPTREND_BREAKOUT_LOOKBACK = 10         # 需突破最近N日收盘高点（不含当日）
MAIN_ACCOUNT_UPTREND_BREAKOUT_BUFFER = 0.003        # 突破缓冲(0.3%)，过滤假突破
MAIN_ACCOUNT_UPTREND_MIN_DAYS_AFTER_ANCHOR = 3      # 锚定日后至少等待N天再追涨
MAIN_ACCOUNT_UPTREND_MAX_DISTANCE_TO_MA20 = 0.50    # 收盘价高于MA20超过该比例则不追（防过度追高）
# 追涨卖出使用与买入A止盈相同的配置和逻辑
# 复用 MAIN_ACCOUNT_TAKE_PROFIT_LEVELS 和 MAIN_ACCOUNT_TAKE_PROFIT_RATIOS
ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT = True  # 使用价ATR倍模式（与止盈一致）
# 动态解锁：价ATR倍连续向0靠近时，允许释放卖出档位锁（复用止盈的动态解锁逻辑）
# 动态解锁：价ATR倍连续向0靠近时，允许释放卖出档位锁
ENABLE_MAIN_ACCOUNT_UPTREND_DYNAMIC_UNLOCK = True
MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS = 2
MAIN_ACCOUNT_UPTREND_UNLOCK_EPS = 0.001
# 买入A止盈：极度远离MA20时锁定止盈（价ATR倍阈值）
MAIN_ACCOUNT_TAKE_PROFIT_EXTREME_LOCK_PRICE_ATR = 4
# 区间交易卖出：极度远离MA20时锁定（适用于追涨/补跌）
ENABLE_MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK = True
MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK_PRICE_ATR = 4
# 区间交易极度买入：空仓且极度符合条件时全仓买入
ENABLE_MAIN_ACCOUNT_EXTREME_BUY_WHEN_EMPTY = True  # 空仓且价ATR倍 >= 阈值时全仓买入
MAIN_ACCOUNT_EXTREME_BUY_PRICE_ATR_THRESHOLD = 4  # 极度买入价ATR倍阈值
MAIN_ACCOUNT_EXTREME_BUY_CONSECUTIVE_DAYS = 1  # 极度买入需要连续满足的天数

# 追涨买入止损阈值：当追涨买入的仓位跌幅超过该阈值时直接卖出
MAIN_ACCOUNT_UPTREND_STOP_LOSS_PCT = -0.10  # 跌幅超过10%时止损卖出
MAIN_ACCOUNT_UPTREND_STOP_LOSS_ON_MA20 = True  # 追涨买入跌破MA20时清仓
# 低于MA20卖出优化配置
MAIN_ACCOUNT_BELOW_MA20_CONSECUTIVE_DAYS = 3  # 连续低于MA20的天数阈值（默认3天）
MAIN_ACCOUNT_BELOW_MA20_THRESHOLD = -0.04  # 低于MA20的阈值（默认3%）
MAIN_ACCOUNT_BELOW_MA20_EMERGENCY_THRESHOLD = -0.08  # 紧急卖出阈值（默认8%，超过直接卖出）

# 买入A延迟买入开关
ENABLE_BUY_A_DELAYED = True  # 是否启用延迟买入
# 新延迟买入规则参数
BUY_A_DELAYED_NEW_ENABLE = True  # 是否启用新的延迟买入规则
BUY_A_DELAYED_NEW_HIT_DAYS = 2  # 价ATR倍 < 5日价ATR倍累计天数
BUY_A_DELAYED_NEW_FIVE_DAY_ATR_THRESHOLD = -1.10  # 5日价ATR倍阈值
BUY_A_DELAYED_NEW_FORCE_BUY_ATR = -3.0  # 强制满仓的价ATR倍阈值

# 追跌买入止损阈值：当追跌买入的三档都买入后，跌幅超过该阈值时直接卖出
MAIN_ACCOUNT_DROP_STOP_LOSS_PCT = -0.10  # 跌幅超过10%时止损卖出
MAIN_ACCOUNT_DROP_STOP_LOSS_REQUIRE_ALL_LEVELS = True  # 是否需要三档都买入后才触发止损



# 买入A与卖出A之间的止盈机制
ENABLE_MAIN_ACCOUNT_TAKE_PROFIT = True  # 是否启用止盈机制
MAIN_ACCOUNT_TAKE_PROFIT_LEVELS = [2, 3, 4]  # 止盈档位（价ATR倍数2/3/4触发）
MAIN_ACCOUNT_TAKE_PROFIT_RATIOS = [0.20, 0.40, 0.50]  # 对应卖出仓位比例（卖出20%/20%/30%/400%）





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


def run_backtest(stock_code: str = STOCK_CODE):
    """回测主函数"""
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 100)
    
    if df is None or len(df) < 60:
        print(f"数据不足，需要至少60天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
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
    
    # 计算波动率相对于上一天的百分比
    df['prev_volatility'] = df['波动率'].shift(1)
    # 计算变化百分比：(当天-前一天)/|前一天| * 100
    df['波动率百分比'] = ((df['波动率'] - df['prev_volatility']) / df['prev_volatility'].abs() * 100).replace([np.inf, -np.inf], np.nan)
    df.drop(columns=['prev_volatility'], inplace=True)
    
    # 计算波幅变化百分比（后一天波幅相对前一天的变化百分比）
    df['prev_波动率百分比'] = df['波动率百分比'].shift(1)
    df['波幅变化%'] = ((df['波动率百分比'] - df['prev_波动率百分比']) / df['prev_波动率百分比'].abs() * 100).replace([np.inf, -np.inf], np.nan)
    df.drop(columns=['prev_波动率百分比'], inplace=True)
    
    # 计算MA20相对于收盘价的幅度百分比
    df['MA20幅度%'] = ((df['ma20'] - df['收盘']) / df['收盘'] * 100).replace([np.inf, -np.inf], np.nan)

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

    # 计算五日收盘最高价和最低价
    df['5日最高'] = df['收盘'].rolling(window=5, min_periods=1).max()
    df['5日最低'] = df['收盘'].rolling(window=5, min_periods=1).min()
    
    # 计算20日收盘最高价
    df['20日最高'] = df['收盘'].rolling(window=20, min_periods=1).max()
    
    # 计算10日最低价及其ATR倍数（滚动窗口方式）
    # 对于每一天，找到最近10天内的最低价，并记录那一天的价ATR倍
    df['10日最低'] = df['收盘'].rolling(window=10, min_periods=1).min()
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
    
    # 买入A与卖出A之间的止盈机制状态变量
    take_profit_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS) if ENABLE_MAIN_ACCOUNT_TAKE_PROFIT else []
    take_profit_ref_price = 0.0  # 止盈参考价（用于相对当前持仓参考价止盈）
    a_take_profit_first_triggered = False
    a_take_profit_prev_distance_to_ma20 = None
    a_take_profit_converge_days = 0
    a_take_profit_extreme_lock_active = False
    a_take_profit_extreme_peak_distance_to_ma20 = None
    a_take_profit_extreme_converge_days = 0
    
    # 卖出A延迟卖出状态变量
    sell_a_delayed_pending = False  # 是否有待卖A（延迟卖出状态）
    sell_a_initial_position = 0  # 卖出A触发时的初始仓位（用于分批卖出计算）
    
    # 买入A延迟买入状态变量
    buy_a_delayed_pending = False  # 是否有待买A
    buy_a_pending_price = 0.0  # 待买A标记价格
    buy_a_pending_mark_price_atr = 0.0  # 待买A标记当天的价ATR倍
    buy_a_below_ma20_atr_hit_count = 0  # 待买A期间，L2累计命中次数
    buy_a_rebound_hit_count = 0  # 待买A期间，L3反弹连续命中次数
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
        main_account_sell_buy_total_shares = 0  # 主账户在卖出A与买入A之间的总买入股数
        # 根据买入方式选择对应的卖出配置长度
        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        else:
            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)  # 主账户卖出A与买入A之间的上涨买入档位
        main_account_drop_anchor_price = 0
        main_account_rise_anchor_price = 0
        main_account_rise_reentry_locked = False
        main_account_had_rise_entry_in_cycle = False
        last_anchor_price = 0
        main_account_anchor_index = -1
        main_account_prev_distance_to_ma20 = None
        main_account_ma20_converge_days = 0
        # 追涨和追跌分别维护独立的加权平均买入价格
        main_account_rise_buy_price = 0  # 追涨买入的加权平均价格
        main_account_rise_buy_shares = 0  # 追涨买入的总股数
        main_account_drop_buy_price = 0  # 追跌买入的加权平均价格
        main_account_initial_cash = 0  # 主账户区间交易的初始资金（卖出时的现金）
        main_account_below_ma20_days = 0  # 连续低于MA20的天数计数器
        main_account_extreme_lock_active = False
        main_account_extreme_peak_price_atr = None
        main_account_extreme_converge_days = 0
        main_account_extreme_buy_consecutive_days = 0  # 极度买入连续满足天数计数器
        main_account_cycle_active = False  # 买入A到卖出A之间的区间交易周期开关
        # 爆发买入机制状态变量
        main_account_outbreak_buy_consecutive_days = 0  # 波动率连续大于阈值天数
        main_account_outbreak_buy_active = False  # 是否处于爆发买入持仓状态
        main_account_outbreak_buy_price = 0  # 爆发买入价格
        main_account_outbreak_sell_high = 0  # 爆发买入后的N日最高价
    else:
        main_account_sell_buy_levels_triggered = []
        main_account_sell_buy_position = 0
        main_account_sell_buy_price = 0
        main_account_sell_buy_total_shares = 0
        main_account_sell_sell_levels_triggered = []
        main_account_uptrend_sell_levels_triggered = []
        main_account_rise_buy_levels_triggered = []
        main_account_drop_anchor_price = 0
        main_account_rise_anchor_price = 0
        main_account_rise_reentry_locked = False
        main_account_had_rise_entry_in_cycle = False
        last_anchor_price = 0
        main_account_anchor_index = -1
        main_account_prev_distance_to_ma20 = None
        main_account_ma20_converge_days = 0
        main_account_rise_buy_price = 0
        main_account_rise_buy_shares = 0
        main_account_drop_buy_price = 0
        main_account_initial_cash = 0
        main_account_below_ma20_days = 0  # 连续低于MA20的天数计数器
        main_account_extreme_lock_active = False
        main_account_extreme_peak_price_atr = None
        main_account_extreme_converge_days = 0
        main_account_extreme_buy_consecutive_days = 0  # 极度买入连续满足天数计数器
        main_account_cycle_active = False
        # 爆发买入机制状态变量
        main_account_outbreak_buy_consecutive_days = 0
        main_account_outbreak_buy_active = False
        main_account_outbreak_buy_price = 0
        main_account_outbreak_sell_high = 0  # 爆发买入后的N日最高价

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
    log_print(f"{'='*175}\n")

    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'ATR14':>8} {'波动率':>8} {'波幅%':>8} {'价ATR倍':>8} {'5日ATR平均':>8} {'10日最低价ATR倍数':>16} {f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高':>8} {'连续天数':>8} {'极度':>4} {'操作':<30} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * 175)
    
    # 遍历每一天进行回测
    for i in range(len(df)):
        row = df.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        ma20 = row['ma20']
        ma20_pct = row['MA20幅度%']  # MA20相对于收盘价的幅度%
        volatility = row['波动率']
        volatility_pct = row['波动率百分比']  # 波幅%
        volatility_change_pct = row['波幅变化%']  # 波幅变化百分比
        
        action = ""
        condition_a = False  # 初始化条件A标记
        
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
            
            # 检查止盈（买入A与卖出A之间）
            take_profit_triggered = False
            take_profit_level = -1
            if position > 0 and ENABLE_MAIN_ACCOUNT_TAKE_PROFIT and take_profit_ref_price > 0:
                # 买入A止盈动态解锁：首次止盈后才启用
                # 同时支持极度远离MA20时锁定止盈，直到场景破坏
                current_price_atr_multiple = row['价ATR倍'] if pd.notna(row['价ATR倍']) else np.nan

                # 极度远离MA20：激活止盈锁
                if pd.notna(current_price_atr_multiple) and current_price_atr_multiple >= MAIN_ACCOUNT_TAKE_PROFIT_EXTREME_LOCK_PRICE_ATR:
                    if not a_take_profit_extreme_lock_active:
                        a_take_profit_extreme_lock_active = True
                        a_take_profit_extreme_peak_distance_to_ma20 = current_price_atr_multiple
                        a_take_profit_extreme_converge_days = 0
                    elif (
                        a_take_profit_extreme_peak_distance_to_ma20 is None
                        or current_price_atr_multiple > a_take_profit_extreme_peak_distance_to_ma20 + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS
                    ):
                        a_take_profit_extreme_peak_distance_to_ma20 = current_price_atr_multiple
                        a_take_profit_extreme_converge_days = 0

                # 锁定后，直到场景破坏（跌回MA20或价ATR倍持续回落）才解锁
                if a_take_profit_extreme_lock_active:
                    if pd.isna(current_price_atr_multiple) or (pd.notna(ma20) and close_price <= ma20):
                        a_take_profit_extreme_lock_active = False
                        a_take_profit_extreme_peak_distance_to_ma20 = None
                        a_take_profit_extreme_converge_days = 0
                    else:
                        if (
                            a_take_profit_extreme_peak_distance_to_ma20 is None
                            or current_price_atr_multiple > a_take_profit_extreme_peak_distance_to_ma20 + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS
                        ):
                            a_take_profit_extreme_peak_distance_to_ma20 = current_price_atr_multiple
                            a_take_profit_extreme_converge_days = 0
                        elif current_price_atr_multiple + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS < a_take_profit_extreme_peak_distance_to_ma20:
                            a_take_profit_extreme_converge_days += 1

                        if a_take_profit_extreme_converge_days >= MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS:
                            a_take_profit_extreme_lock_active = False
                            a_take_profit_extreme_peak_distance_to_ma20 = None
                            a_take_profit_extreme_converge_days = 0

                # 仅在未被极端拉伸锁定时，才执行止盈触发判断
                if not a_take_profit_extreme_lock_active:
                    if (
                        ENABLE_MAIN_ACCOUNT_UPTREND_DYNAMIC_UNLOCK
                        and a_take_profit_first_triggered
                        and pd.notna(ma20)
                        and ma20 > 0
                        and close_price > ma20
                    ):
                        current_distance_to_ma20 = (close_price - ma20) / ma20
                        if a_take_profit_prev_distance_to_ma20 is not None and (
                            current_distance_to_ma20 + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS < a_take_profit_prev_distance_to_ma20
                        ):
                            a_take_profit_converge_days += 1
                        else:
                            a_take_profit_converge_days = 0
                        a_take_profit_prev_distance_to_ma20 = current_distance_to_ma20
                        if (
                            a_take_profit_converge_days >= MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS
                            and len(take_profit_levels_triggered) > 0
                        ):
                            # 动态解锁：依次重置已触发的止盈档位（止盈1和止盈2可以反复触发）
                            for tp_idx in range(len(take_profit_levels_triggered)):
                                if take_profit_levels_triggered[tp_idx]:
                                    take_profit_levels_triggered[tp_idx] = False
                                    break  # 只重置第一个已触发的档位
                    else:
                        a_take_profit_prev_distance_to_ma20 = None
                        a_take_profit_converge_days = 0

                    # 计算价ATR倍数
                    current_price_atr_multiple = 0
                    if pd.notna(ma20) and ma20 > 0 and pd.notna(df.loc[i, 'ATR']) and df.loc[i, 'ATR'] > 0:
                        current_price_atr_multiple = (close_price - ma20) / df.loc[i, 'ATR']
                    
                    # 遍历止盈档位，检查是否触发止盈（支持同时触发多个档位）
                    newly_triggered_tp_levels = []
                    for tp_idx, tp_level in enumerate(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS):
                        if not take_profit_levels_triggered[tp_idx] and current_price_atr_multiple >= tp_level and close_price > buy_price:
                            newly_triggered_tp_levels.append(tp_idx)
                    
                    if newly_triggered_tp_levels:
                        take_profit_triggered = True
                        take_profit_level = newly_triggered_tp_levels[-1]  # 最高触发的档位
            # 原始卖出A信号：波动率>0且降低，且降至前一天阈值以下
            sell_a_signal_triggered = False
            if volatility > 0 and is_volatility_declining:
                volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                if volatility_ratio <= SELL_RATIO_THRESHOLD:
                    sell_a_signal_triggered = True
            
            # 卖出A延迟卖出逻辑
            if ENABLE_SELL_A_DELAYED and position > 0:
                # 启用延迟卖出
                if sell_a_signal_triggered and not sell_a_delayed_pending:
                    # 首次触发卖出A信号，进入延迟卖出状态
                    sell_a_delayed_pending = True
                    sell_a_initial_position = position
                
                # 如果处于延迟卖出状态，每天检查价ATR倍与5日平均的关系
                if sell_a_delayed_pending:
                    # 获取当前价ATR倍数和5日平均
                    current_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
                    ma5_price_atr = row['5日价ATR平均'] if pd.notna(row['5日价ATR平均']) else 0
                    
                    if current_price_atr < ma5_price_atr:
                        # 价ATR倍 < 5日平均，全仓卖出
                        should_sell = True
                        sell_reason = "比率卖出(延迟-全仓)"
                    else:
                        # 价ATR倍 > 5日平均，分批卖出（每次卖出初始仓位的25%）
                        sell_shares_delayed = int(sell_a_initial_position * SELL_A_DELAYED_RATIO_THRESHOLD / 100) * 100
                        sell_shares_delayed = min(sell_shares_delayed, position)
                        sell_shares_delayed = max(sell_shares_delayed, 100)  # 至少卖100股
                        
                        if sell_shares_delayed > 0:
                            sell_price = close_price
                            sell_value = sell_shares_delayed * sell_price
                            profit = (sell_price - buy_price) * sell_shares_delayed
                            cash += sell_value
                            position_before_sell = position
                            position -= sell_shares_delayed
                            trade_count += 1
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '卖出',
                                'price': sell_price,
                                'shares': sell_shares_delayed,
                                'profit': profit
                            })
                            action = f"卖出A延迟分批@{sell_price:.2f} 持仓{position_before_sell}→{position}"
                            
                            if position <= 0:
                                # 已全部卖完，重置状态
                                position = 0
                                buy_price = 0
                                sell_a_delayed_pending = False
                                sell_a_initial_position = 0
                                should_sell = True  # 标记为已完全卖出
                                sell_reason = "比率卖出(延迟-分批完成)"
            elif sell_a_signal_triggered and position > 0:
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
                    # 重置止盈档位
                    if ENABLE_MAIN_ACCOUNT_TAKE_PROFIT:
                        take_profit_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)

                    a_take_profit_first_triggered = False
                    a_take_profit_prev_distance_to_ma20 = None
                    a_take_profit_converge_days = 0
                    a_take_profit_extreme_lock_active = False
                    a_take_profit_extreme_peak_distance_to_ma20 = None
                    a_take_profit_extreme_converge_days = 0
                    take_profit_ref_price = 0.0
                    buy_a_delayed_pending = False
                    buy_a_marked = False
                    buy_a_pending_price = 0.0
                    buy_a_pending_mark_price_atr = 0.0
                    buy_a_below_ma20_atr_hit_count = 0
                    buy_a_rebound_hit_count = 0
                    # 重置卖出A延迟卖出状态
                    sell_a_delayed_pending = False
                    sell_a_initial_position = 0
                    
                    # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                        # 设置锚定价格为卖出价格
                        last_anchor_price = sell_price
                        main_account_anchor_index = i
                        main_account_drop_anchor_price = sell_price
                        main_account_rise_anchor_price = sell_price
                        main_account_rise_reentry_locked = False
                        main_account_had_rise_entry_in_cycle = False
                        main_account_prev_distance_to_ma20 = None
                        main_account_ma20_converge_days = 0
                        main_account_extreme_lock_active = False
                        main_account_extreme_peak_price_atr = None
                        main_account_extreme_converge_days = 0
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
                        main_account_sell_buy_total_shares = 0
                        # 根据买入方式选择对应的卖出配置长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        else:
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS)
                        main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                        # 重置追涨/追跌的独立加权平均价格
                        main_account_rise_buy_price = 0
                        main_account_rise_buy_shares = 0
                        main_account_drop_buy_price = 0
                elif take_profit_triggered and take_profit_level >= 0:
                    # 止盈卖出：部分卖出（支持同时触发多个档位）
                    # 计算所有新触发档位的总卖出比例
                    total_tp_ratio = sum(MAIN_ACCOUNT_TAKE_PROFIT_RATIOS[tp_idx] for tp_idx in newly_triggered_tp_levels)
                    sell_shares = int(position * total_tp_ratio)
                    sell_shares = min(sell_shares, position)
                    sell_shares = max(sell_shares, 0)
                    
                    if sell_shares > 0:
                        position_before_sell = position
                        sell_price = close_price
                        sell_value = sell_shares * sell_price
                        profit = (sell_price - buy_price) * sell_shares
                        cash += sell_value
                        position -= sell_shares
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '卖出',
                            'price': sell_price,
                            'shares': sell_shares,
                            'profit': profit
                        })
                        # 标记所有触发的档位
                        for tp_idx in newly_triggered_tp_levels:
                            take_profit_levels_triggered[tp_idx] = True
                        # 构建操作描述
                        if len(newly_triggered_tp_levels) > 1:
                            tp_levels_str = ','.join([f"止盈{tp_idx + 1}" for tp_idx in newly_triggered_tp_levels])
                        else:
                            tp_levels_str = f"止盈{newly_triggered_tp_levels[0] + 1}"
                        action = f"{tp_levels_str}@{sell_price:.2f} 持仓{position_before_sell}→{position}"
                        a_take_profit_first_triggered = True
                        if position > 0:
                            # 止盈后更新参考价，后续止盈相对当前持仓参考价判断
                            take_profit_ref_price = sell_price
                        else:
                            take_profit_ref_price = 0.0
                        if pd.notna(ma20) and ma20 > 0:
                            a_take_profit_prev_distance_to_ma20 = (close_price - ma20) / ma20
                            a_take_profit_converge_days = 0

            
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
                        buy_a_pending_mark_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0.0
                        buy_a_below_ma20_atr_hit_count = 0
                        buy_a_rebound_hit_count = 0
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
                            main_account_sell_buy_total_shares = 0
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
                            main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                        new_position = int(cash / buy_price / 100) * 100
                        if new_position >= 100:
                            position = new_position
                            cash -= position * buy_price
                            trade_count += 1
                            actual_condition_a_buy_count += 1
                            action = f"买入A@{buy_price:.2f}"
                            take_profit_ref_price = buy_price
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'shares': position
                            })
                            if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                                main_account_cycle_active = True
                                main_account_initial_cash = cash
                                last_anchor_price = buy_price
                                main_account_anchor_index = i
                                main_account_drop_anchor_price = buy_price
                                main_account_rise_anchor_price = buy_price
                                main_account_rise_reentry_locked = False
                                main_account_had_rise_entry_in_cycle = False
                                main_account_prev_distance_to_ma20 = None
                                main_account_ma20_converge_days = 0
                                main_account_extreme_lock_active = False
                                main_account_extreme_peak_price_atr = None
                                main_account_extreme_converge_days = 0
                                main_account_extreme_buy_consecutive_days = 0
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
                                main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                                main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                                main_account_sell_buy_position = 0
                                main_account_sell_buy_price = 0
                                main_account_sell_buy_total_shares = 0
                                main_account_rise_buy_price = 0
                                main_account_rise_buy_shares = 0
                                main_account_drop_buy_price = 0
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
                    buy_a_pending_mark_price_atr = 0.0
                    buy_a_below_ma20_atr_hit_count = 0
                    buy_a_rebound_hit_count = 0
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
                        main_account_sell_buy_total_shares = 0
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
                        main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                    new_position = int(cash / buy_price / 100) * 100
                    if new_position >= 100:
                        position = new_position
                        cash -= position * buy_price
                        trade_count += 1
                        actual_condition_a_buy_count += 1
                        action = f"买入A@{buy_price:.2f}(由待买A触发)"
                        take_profit_ref_price = buy_price
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
                        buy_a_pending_mark_price_atr = 0.0
                        buy_a_below_ma20_atr_hit_count = 0
                        buy_a_rebound_hit_count = 0
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                            main_account_cycle_active = True
                            main_account_initial_cash = cash
                            last_anchor_price = buy_price
                            main_account_anchor_index = i
                            main_account_drop_anchor_price = buy_price
                            main_account_rise_anchor_price = buy_price
                            main_account_rise_reentry_locked = False
                            main_account_had_rise_entry_in_cycle = False
                            main_account_prev_distance_to_ma20 = None
                            main_account_ma20_converge_days = 0
                            main_account_extreme_lock_active = False
                            main_account_extreme_peak_price_atr = None
                            main_account_extreme_converge_days = 0
                            main_account_extreme_buy_consecutive_days = 0
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
                            main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
                            main_account_rise_buy_price = 0
                            main_account_rise_buy_shares = 0
                            main_account_drop_buy_price = 0
                        volatility_declining_days = 0
                        if holding_start_date is None:
                            holding_start_date = date_str


        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and position == 0 and (not buy_a_delayed_pending) and (main_account_drop_anchor_price > 0 or main_account_rise_anchor_price > 0):
            drop_anchor_price = main_account_drop_anchor_price if main_account_drop_anchor_price > 0 else last_anchor_price
            rise_anchor_price = main_account_rise_anchor_price if main_account_rise_anchor_price > 0 else last_anchor_price
            price_drop_pct = (close_price - drop_anchor_price) / drop_anchor_price if drop_anchor_price > 0 else 0
            
            # 标记当天是否有买入操作
            has_buy_today = False
            
            # 极度买入：空仓且极度符合条件时全仓买入（需要连续满足天数）
            if ENABLE_MAIN_ACCOUNT_EXTREME_BUY_WHEN_EMPTY and main_account_sell_buy_position == 0:
                current_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
                if current_price_atr >= MAIN_ACCOUNT_EXTREME_BUY_PRICE_ATR_THRESHOLD:
                    main_account_extreme_buy_consecutive_days += 1
                    # 达到连续天数要求才买入
                    if main_account_extreme_buy_consecutive_days >= MAIN_ACCOUNT_EXTREME_BUY_CONSECUTIVE_DAYS:
                        # 全仓买入
                        new_position = int(cash / close_price / 100) * 100
                        if new_position >= 100 and cash >= new_position * close_price:
                            cost = new_position * close_price
                            cash -= cost
                            main_account_sell_buy_position = new_position
                            main_account_sell_buy_price = close_price
                            main_account_sell_buy_total_shares = new_position
                            main_account_rise_buy_price = close_price
                            main_account_rise_buy_shares = new_position
                            main_account_had_rise_entry_in_cycle = True
                            trade_count += 1
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': close_price,
                                'shares': new_position,
                                'type': '极度买入'
                            })
                            action = f"主账户极度买入@{close_price:.2f} 持仓{new_position}"
                            has_buy_today = True
                            # 记录持仓开始日期
                            if holding_start_date is None:
                                holding_start_date = date_str
                            # 买入后重置计数器
                            main_account_extreme_buy_consecutive_days = 0
                else:
                    # 不满足条件，重置计数器
                    main_account_extreme_buy_consecutive_days = 0

            # 爆发买入机制：价ATR倍连续大于阈值时买入（无论是否有持仓）
            if ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY and not has_buy_today and not main_account_outbreak_buy_active:
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
                            main_account_sell_buy_total_shares += new_position
                            main_account_outbreak_buy_price = close_price
                            main_account_outbreak_buy_active = True
                            # 初始化N日最高价为当前N日最高
                            high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                            main_account_outbreak_sell_high = row[high_col] if pd.notna(row[high_col]) else close_price
                            # 重置其他买入状态
                            main_account_had_rise_entry_in_cycle = False
                            main_account_rise_buy_price = 0
                            main_account_rise_buy_shares = 0
                            main_account_drop_buy_price = 0
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
                            has_buy_today = True
                            # 记录持仓开始日期
                            if holding_start_date is None:
                                holding_start_date = date_str
                            # 买入后重置计数器
                            main_account_outbreak_buy_consecutive_days = 0
                else:
                    # 不满足条件，重置计数器
                    main_account_outbreak_buy_consecutive_days = 0

            if ENABLE_MAIN_ACCOUNT_UPTREND_BUY and (not main_account_rise_reentry_locked) and rise_anchor_price > 0:
                prev_ma20 = df.iloc[i-1]['ma20'] if i > 0 and pd.notna(df.iloc[i-1]['ma20']) else ma20
                prev2_ma20 = df.iloc[i-2]['ma20'] if i > 1 and pd.notna(df.iloc[i-2]['ma20']) else prev_ma20
                ma20_up_strong = pd.notna(ma20) and pd.notna(prev_ma20) and pd.notna(prev2_ma20) and (ma20 > prev_ma20 > prev2_ma20)
                ma20_trend_ok = ma20_up_strong if MAIN_ACCOUNT_UPTREND_REQUIRE_MA20_UP else True
                days_since_anchor = (i - main_account_anchor_index) if main_account_anchor_index >= 0 else 9999
                anchor_wait_ok = days_since_anchor >= MAIN_ACCOUNT_UPTREND_MIN_DAYS_AFTER_ANCHOR
                lookback_start = max(0, i - MAIN_ACCOUNT_UPTREND_BREAKOUT_LOOKBACK)
                recent_close_high = df.iloc[lookback_start:i]['收盘'].max() if i > lookback_start else close_price
                breakout_ok = pd.notna(recent_close_high) and close_price >= recent_close_high * (1 + MAIN_ACCOUNT_UPTREND_BREAKOUT_BUFFER)
                distance_to_ma20 = ((close_price - ma20) / ma20) if pd.notna(ma20) and ma20 > 0 else 0
                not_too_far_from_ma20 = distance_to_ma20 <= MAIN_ACCOUNT_UPTREND_MAX_DISTANCE_TO_MA20
                allow_uptrend_buy = (
                    pd.notna(ma20)
                    and close_price > ma20
                    and ma20_trend_ok
                    and anchor_wait_ok
                    and breakout_ok
                    and not_too_far_from_ma20
                )
                if allow_uptrend_buy:
                    rise_pct = (close_price - rise_anchor_price) / rise_anchor_price if rise_anchor_price > 0 else 0
                    triggered_rise_levels = []
                    executed_rise_levels = []
                    for rise_idx, (rise_level, rise_ratio) in enumerate(zip(MAIN_ACCOUNT_UPTREND_LEVELS, MAIN_ACCOUNT_UPTREND_RATIOS)):
                        if not main_account_rise_buy_levels_triggered[rise_idx] and rise_pct >= rise_level:
                            triggered_rise_levels.append((rise_idx, rise_ratio))

                    for rise_idx, rise_ratio in triggered_rise_levels:
                        buy_amount = main_account_initial_cash * rise_ratio
                        buy_amount = min(buy_amount, cash)
                        new_position = int(buy_amount / close_price / 100) * 100
                        if new_position >= 100 and cash >= new_position * close_price:
                            cost = new_position * close_price
                            cash -= cost
                            # 更新总持仓的加权平均价格
                            if main_account_sell_buy_position == 0:
                                main_account_sell_buy_price = close_price
                            else:
                                main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
                            # 更新追涨买入的独立加权平均价格
                            if main_account_rise_buy_price == 0:
                                main_account_rise_buy_price = close_price
                            else:
                                main_account_rise_buy_price = (main_account_rise_buy_price * main_account_rise_buy_shares + close_price * new_position) / (main_account_rise_buy_shares + new_position)
                            main_account_rise_buy_shares += new_position
                            main_account_sell_buy_position += new_position
                            main_account_sell_buy_total_shares += new_position
                            trade_count += 1
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': close_price,
                                'shares': new_position,
                                'level': rise_idx + 1
                            })
                            main_account_rise_buy_levels_triggered[rise_idx] = True
                            main_account_had_rise_entry_in_cycle = True
                            main_account_prev_distance_to_ma20 = ((close_price - ma20) / ma20) if pd.notna(ma20) and ma20 > 0 else None
                            main_account_ma20_converge_days = 0
                            executed_rise_levels.append(rise_idx)
                            # 记录持仓开始日期（区间交易首次买入）
                            if holding_start_date is None:
                                holding_start_date = date_str

                    if executed_rise_levels:
                        rise_levels_str = ",".join([f"追涨买{idx+1}" for idx in executed_rise_levels])
                        # 在买入输出中增加加权价格信息
                        rise_buy_price_info = f" 加权价={main_account_rise_buy_price:.2f}" if main_account_rise_buy_price > 0 else ""
                        action = f"主账户{rise_levels_str}@{close_price:.2f}{rise_buy_price_info} 持仓{main_account_sell_buy_position}"
                        has_buy_today = True
                        # 重置卖出档位标记
                        if ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT:
                            main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                        else:
                            main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
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
                        # 更新追跌买入的独立加权平均价格
                        if main_account_drop_buy_price == 0:
                            main_account_drop_buy_price = close_price
                        else:
                            # 简化处理，直接用当前持仓计算加权平均
                            main_account_drop_buy_price = (main_account_drop_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)

                        main_account_sell_buy_position += new_position
                        main_account_sell_buy_total_shares += new_position
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
                        # 更新追跌买入的独立加权平均价格
                        if main_account_drop_buy_price == 0:
                            main_account_drop_buy_price = close_price
                        else:
                            # 简化处理，直接用当前持仓计算加权平均
                            main_account_drop_buy_price = (main_account_drop_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)

                        main_account_sell_buy_position += new_position
                        main_account_sell_buy_total_shares += new_position
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
                # 追涨卖出锁定动态解锁逻辑
                if (
                    ENABLE_MAIN_ACCOUNT_UPTREND_DYNAMIC_UNLOCK
                    and main_account_had_rise_entry_in_cycle
                    and pd.notna(ma20)
                    and ma20 > 0
                    and close_price > ma20
                ):
                    current_distance_to_ma20 = (close_price - ma20) / ma20
                    if main_account_prev_distance_to_ma20 is not None and (
                        current_distance_to_ma20 + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS < main_account_prev_distance_to_ma20
                    ):
                        main_account_ma20_converge_days += 1
                    else:
                        main_account_ma20_converge_days = 0
                    main_account_prev_distance_to_ma20 = current_distance_to_ma20
                    if (
                        main_account_ma20_converge_days >= MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS
                        and len(main_account_uptrend_sell_levels_triggered) > 0
                    ):
                        # 动态解锁：依次重置已触发的卖出档位（与止盈逻辑一致）
                        for sell_idx in range(len(main_account_uptrend_sell_levels_triggered)):
                            if main_account_uptrend_sell_levels_triggered[sell_idx]:
                                main_account_uptrend_sell_levels_triggered[sell_idx] = False
                                break  # 只重置第一个已触发的档位
                else:
                    main_account_prev_distance_to_ma20 = None
                    main_account_ma20_converge_days = 0
                # 计算当前持仓的盈亏百分比（使用总持仓的平均价格）
                # 区间仓极端拉伸锁：价ATR倍过高时锁定卖出，防止过早止盈
                current_cycle_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else np.nan
                if ENABLE_MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK:
                    if pd.notna(current_cycle_price_atr) and current_cycle_price_atr >= MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK_PRICE_ATR:
                        if not main_account_extreme_lock_active:
                            main_account_extreme_lock_active = True
                            main_account_extreme_peak_price_atr = current_cycle_price_atr
                            main_account_extreme_converge_days = 0
                        elif (
                            main_account_extreme_peak_price_atr is None
                            or current_cycle_price_atr > main_account_extreme_peak_price_atr + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS
                        ):
                            main_account_extreme_peak_price_atr = current_cycle_price_atr
                            main_account_extreme_converge_days = 0

                    if main_account_extreme_lock_active:
                        if pd.isna(current_cycle_price_atr) or (pd.notna(ma20) and close_price <= ma20):
                            main_account_extreme_lock_active = False
                            main_account_extreme_peak_price_atr = None
                            main_account_extreme_converge_days = 0
                        else:
                            if (
                                main_account_extreme_peak_price_atr is None
                                or current_cycle_price_atr > main_account_extreme_peak_price_atr + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS
                            ):
                                main_account_extreme_peak_price_atr = current_cycle_price_atr
                                main_account_extreme_converge_days = 0
                            elif current_cycle_price_atr + MAIN_ACCOUNT_UPTREND_UNLOCK_EPS < main_account_extreme_peak_price_atr:
                                main_account_extreme_converge_days += 1

                            if main_account_extreme_converge_days >= MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS:
                                main_account_extreme_lock_active = False
                                main_account_extreme_peak_price_atr = None
                                main_account_extreme_converge_days = 0

                position_profit_pct = (close_price - main_account_sell_buy_price) / main_account_sell_buy_price
                
                # 检查是否触发止损（追涨或追跌）
                stop_loss_triggered = False
                stop_loss_type = None  # 'rise' 或 'drop' 或 'ma20' 或 'outbreak'
                
                # 爆发买入锁定：当爆发买入激活时，只检查爆发卖出条件，跳过其他卖出机制
                if main_account_outbreak_buy_active:
                    # 检查爆发买入卖出条件：N日最高价下降超过阈值才卖出（防止小幅波动）
                    high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                    current_high = row[high_col] if pd.notna(row[high_col]) else close_price
                    if current_high > main_account_outbreak_sell_high:
                        # 创新高，更新N日最高价
                        main_account_outbreak_sell_high = current_high
                    elif main_account_outbreak_sell_high > 0:
                        # 计算N日最高价下降比例
                        drop_pct = (main_account_outbreak_sell_high - current_high) / main_account_outbreak_sell_high
                        if drop_pct > MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD:
                            # 下降超过阈值，触发卖出
                            stop_loss_triggered = True
                            stop_loss_type = 'outbreak'
                else:
                    # 检查追涨止损
                    if main_account_had_rise_entry_in_cycle and main_account_rise_buy_price > 0:
                        rise_profit_pct = (close_price - main_account_rise_buy_price) / main_account_rise_buy_price
                        if rise_profit_pct <= MAIN_ACCOUNT_UPTREND_STOP_LOSS_PCT:
                            stop_loss_triggered = True
                            stop_loss_type = 'rise'
                        # 检查追涨买入跌破MA20清仓（优化版：连续天数+紧急卖出阈值）
                        elif MAIN_ACCOUNT_UPTREND_STOP_LOSS_ON_MA20 and pd.notna(ma20) and ma20 > 0:
                            distance_to_ma20_pct = (close_price - ma20) / ma20
                            # 紧急卖出：跌幅超过紧急阈值（如8%）直接卖出
                            if distance_to_ma20_pct <= MAIN_ACCOUNT_BELOW_MA20_EMERGENCY_THRESHOLD:
                                stop_loss_triggered = True
                                stop_loss_type = 'ma20'
                                main_account_below_ma20_days = 0  # 重置计数器
                            # 普通卖出：连续低于MA20阈值达到指定天数
                            elif distance_to_ma20_pct <= MAIN_ACCOUNT_BELOW_MA20_THRESHOLD:
                                main_account_below_ma20_days += 1
                                if main_account_below_ma20_days >= MAIN_ACCOUNT_BELOW_MA20_CONSECUTIVE_DAYS:
                                    stop_loss_triggered = True
                                    stop_loss_type = 'ma20'
                                    main_account_below_ma20_days = 0  # 重置计数器
                            else:
                                # 价格回到MA20阈值之上，重置计数器
                                main_account_below_ma20_days = 0
                    
                    # 检查追跌止损（需要三档都买入后才触发）
                    if not stop_loss_triggered and main_account_drop_buy_price > 0:
                        # 检查是否所有追跌档位都已触发
                        all_drop_levels_triggered = all(main_account_sell_buy_levels_triggered)
                        if all_drop_levels_triggered or not MAIN_ACCOUNT_DROP_STOP_LOSS_REQUIRE_ALL_LEVELS:
                            drop_profit_pct = (close_price - main_account_drop_buy_price) / main_account_drop_buy_price
                            if drop_profit_pct <= MAIN_ACCOUNT_DROP_STOP_LOSS_PCT:
                                stop_loss_triggered = True
                                stop_loss_type = 'drop'
                
                current_atr = row['atr14'] if pd.notna(row['atr14']) else 0
                rise_profit_for_sell = (
                    (close_price - main_account_rise_buy_price) / main_account_rise_buy_price
                    if main_account_rise_buy_price > 0 else 0
                )

                # 追涨卖出使用与买入A止盈相同的逻辑：基于价ATR倍（相对MA20）
                # 计算基于MA20的价ATR倍数（与止盈机制一致）
                ma20_price_atr_multiplier = 0
                if pd.notna(ma20) and ma20 > 0 and current_atr > 0:
                    ma20_price_atr_multiplier = (close_price - ma20) / current_atr
                
                # 爆发买入锁定：当爆发买入激活时，跳过正常的档位卖出逻辑
                can_evaluate_sell = stop_loss_triggered or (
                    (not main_account_outbreak_buy_active)  # 爆发买入时锁定其他卖出
                    and (not main_account_extreme_lock_active)
                    and (current_atr > 0 and ma20_price_atr_multiplier > 0)
                )
                if can_evaluate_sell:
                    price_atr_multiplier = (close_price - main_account_sell_buy_price) / current_atr if current_atr > 0 else 0

                    if main_account_had_rise_entry_in_cycle:
                        # 追涨卖出复用止盈的档位和比例配置
                        sell_levels = MAIN_ACCOUNT_TAKE_PROFIT_LEVELS
                        sell_trigger_value = ma20_price_atr_multiplier
                        sell_ratios = MAIN_ACCOUNT_TAKE_PROFIT_RATIOS
                    else:
                        # 根据买入方式选择对应的卖出配置
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            sell_levels = MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS
                            # ATR买入体系的卖出也用价ATR倍（相对MA20），与打印列一致
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
                        if main_account_had_rise_entry_in_cycle:
                            # 追涨买入的卖出档位标记
                            trigger_cap = min(max_triggered_level + 1, len(main_account_uptrend_sell_levels_triggered))
                            for idx in range(trigger_cap):
                                if not main_account_uptrend_sell_levels_triggered[idx]:
                                    main_account_uptrend_sell_levels_triggered[idx] = True
                                    newly_triggered_levels.append(idx)
                        else:
                            # 追跌买入的卖出档位标记
                            trigger_cap = min(max_triggered_level + 1, len(main_account_sell_sell_levels_triggered))
                            for idx in range(trigger_cap):
                                if not main_account_sell_sell_levels_triggered[idx]:
                                    main_account_sell_sell_levels_triggered[idx] = True
                                    newly_triggered_levels.append(idx)
                    
                    trade_level = 0
                    # reset rise/drop independent prices
                    if stop_loss_triggered:
                        if stop_loss_type in ('rise', 'ma20'):
                            # 止损时卖出所有追涨买入的仓位（包括跌破MA20清仓）
                            sell_shares = main_account_sell_buy_position
                            sell_shares = max(sell_shares, 0)
                        elif stop_loss_type == 'drop':
                            # 止损时卖出所有追跌买入的仓位
                            sell_shares = main_account_sell_buy_position
                            sell_shares = max(sell_shares, 0)
                        elif stop_loss_type == 'outbreak':
                            # 爆发买入卖出所有仓位
                            sell_shares = main_account_sell_buy_position
                            sell_shares = max(sell_shares, 0)
                        else:
                            sell_shares = 0
                    elif main_account_had_rise_entry_in_cycle or newly_triggered_levels:
                        if newly_triggered_levels:
                            step_ratio = sum(sell_ratios[idx] for idx in newly_triggered_levels if idx < len(sell_ratios))
                            sell_shares = int(main_account_sell_buy_position * step_ratio)
                            sell_shares = min(sell_shares, main_account_sell_buy_position)
                            trade_level = newly_triggered_levels[-1] + 1
                        else:
                            sell_shares = 0
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
                        # 按比例更新追涨买入股数
                        if main_account_rise_buy_shares > 0:
                            main_account_rise_buy_shares -= sell_shares
                            if main_account_rise_buy_shares < 0:
                                main_account_rise_buy_shares = 0
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
                            if stop_loss_type == 'rise':
                                triggered_levels = "止损(追涨)"
                            elif stop_loss_type == 'ma20':
                                triggered_levels = "止损(MA20)"
                            elif stop_loss_type == 'drop':
                                triggered_levels = "止损(追跌)"
                            elif stop_loss_type == 'outbreak':
                                triggered_levels = "爆发卖出"
                            else:
                                triggered_levels = "止损"
                        else:
                            if newly_triggered_levels:
                                sell_source = "追涨" if main_account_had_rise_entry_in_cycle else "追跌"
                                triggered_levels = ",".join([f"{sell_source}卖{i+1}" for i in newly_triggered_levels])
                            else:
                                sell_source = "追涨" if main_account_had_rise_entry_in_cycle else "追跌"
                                triggered_levels = ",".join([f"{sell_source}卖{i+1}" for i in range(max_triggered_level + 1)])
                        remaining_position = main_account_sell_buy_position
                        # 在卖出输出中增加加权价格信息
                        rise_buy_price_info = f" 加权价={main_account_rise_buy_price:.2f}" if main_account_rise_buy_price > 0 else ""
                        action = f"主账户{triggered_levels}@{close_price:.2f}{rise_buy_price_info} 持仓{remaining_position}"
                        # 卖出后重置买入档位，允许在更低价格继续追跌买入
                        # 根据开关选择买入触发方式对应的数组长度
                        if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        else:
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
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
                            main_account_uptrend_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS)
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                            # reset rise/drop independent prices
                            main_account_rise_buy_price = 0
                            main_account_rise_buy_shares = 0
                            main_account_drop_buy_price = 0
                            if main_account_had_rise_entry_in_cycle:
                                main_account_rise_reentry_locked = True
                            main_account_had_rise_entry_in_cycle = False
                            main_account_prev_distance_to_ma20 = None
                            main_account_ma20_converge_days = 0
                            # reset rise/drop independent prices
                            holding_start_date = None
                            # 重置低于MA20天数计数器
                            main_account_below_ma20_days = 0
                            main_account_extreme_lock_active = False
                            main_account_extreme_peak_price_atr = None
                            main_account_extreme_converge_days = 0
                            main_account_extreme_buy_consecutive_days = 0  # 重置极度买入计数器
                            # 重置爆发买入状态
                            main_account_outbreak_buy_active = False
                            main_account_outbreak_buy_price = 0
                            main_account_outbreak_buy_consecutive_days = 0
                            main_account_outbreak_sell_high = 0  # 重置N日最高价
                        else:
                            # reset rise/drop independent prices
                            if stop_loss_type in ('rise', 'ma20'):
                                # reset rise/drop independent prices
                                main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                                main_account_rise_buy_price = 0
                                main_account_rise_buy_shares = 0
                                if main_account_had_rise_entry_in_cycle:
                                    main_account_rise_reentry_locked = True
                                main_account_had_rise_entry_in_cycle = False
                                main_account_prev_distance_to_ma20 = None
                                main_account_ma20_converge_days = 0
                                # 重置低于MA20天数计数器
                                main_account_below_ma20_days = 0
                                main_account_extreme_buy_consecutive_days = 0  # 重置极度买入计数器
                            elif stop_loss_type == 'drop':
                                # reset rise/drop independent prices
                                # 根据开关选择买入触发方式对应的数组长度
                                if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                                    main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                                else:
                                    main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                                main_account_drop_buy_price = 0
                                main_account_drop_anchor_price = close_price  # update drop anchor after sell
                                main_account_extreme_lock_active = False
                                main_account_extreme_peak_price_atr = None
                                main_account_extreme_converge_days = 0
                                main_account_extreme_buy_consecutive_days = 0  # 重置极度买入计数器
                            elif stop_loss_type == 'outbreak':
                                # 重置爆发买入状态
                                main_account_outbreak_buy_active = False
                                main_account_outbreak_buy_price = 0
                                main_account_outbreak_buy_consecutive_days = 0
                                main_account_outbreak_sell_high = 0  # 重置N日最高价

        display_position = position
        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
            display_position += main_account_sell_buy_position
        market_value = cash + display_position * close_price if display_position > 0 else cash
        position_str = f"{display_position}" if display_position > 0 else "0"
        
        # 数据显示（所有情况都显示，包括可买未买）
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        atr14_str = f"{row['atr14']:.2f}" if pd.notna(row['atr14']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        volatility_pct_str = f"{row['波动率百分比']:.1f}" if pd.notna(row['波动率百分比']) else "N/A"
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
        
        # 标记极度远离MA20的情况（价ATR倍 >= 4.0）
        price_atr_value = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
        extreme_marker = "★" if price_atr_value >= MAIN_ACCOUNT_TAKE_PROFIT_EXTREME_LOCK_PRICE_ATR else ""

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {atr14_str:>8} {volatility_str:>8} {volatility_pct_str:>8} {price_atr_ratio_str:>8} {five_day_atr_avg_str:>8} {ten_day_low_atr_str:>12} {n_day_high_str:>8} {consecutive_days_str:>8} {extreme_marker:>4} {action:<30} {position_str:>8} {market_value:>12,.2f}")
    
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
        last_ma20_pct = last_row['MA20幅度%']
        
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


