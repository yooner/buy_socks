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

# 主账户区间交易分批买入配置（相对于锚定价格的跌幅）
MAIN_ACCOUNT_BUY_LEVELS = [-0.04, -0.08, -0.13]  # 买入触发跌幅（-4%, -8%, -13%）
MAIN_ACCOUNT_BUY_RATIOS = [0.20, 0.30, 0.50]      # 对应买入比例（20%, 30%, 50%）

# 主账户区间交易分批卖出配置（相对于买入成本的ATR倍数）
MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS = [1.0, 1.5, 2.0, 3.0]  # 卖出触发ATR倍数（1.0, 1.5, 2.0）
MAIN_ACCOUNT_SELL_RATIOS = [0.30, 0.30, 0.25, 0.15]          # 对应卖出比例（30%, 30%, 40%）

# 主账户区间交易：剩余仓位小于等于该阈值时直接清仓，避免长期残仓
MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR = 300
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
MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS = [1.1, 2.0, 3.0, 3.8]
MAIN_ACCOUNT_UPTREND_SELL_RATIOS = [0.40, 0.35, 0.25]
ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT = True
MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS = [0.10, 0.20, 0.40]
# 动态解锁：价ATR倍连续向0靠近时，允许释放卖出档位锁
ENABLE_MAIN_ACCOUNT_UPTREND_DYNAMIC_UNLOCK = True
MAIN_ACCOUNT_UPTREND_UNLOCK_CONVERGE_DAYS = 2
MAIN_ACCOUNT_UPTREND_UNLOCK_EPS = 0.001
# 买入A止盈：极度远离MA20时锁定止盈（价ATR倍阈值）
MAIN_ACCOUNT_TAKE_PROFIT_EXTREME_LOCK_PRICE_ATR = 4.0
# 区间交易卖出：极度远离MA20时锁定（适用于追涨/补跌）
ENABLE_MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK = True
MAIN_ACCOUNT_SELL_BUY_EXTREME_LOCK_PRICE_ATR = 4.0
# 区间交易极度买入：空仓且极度符合条件时全仓买入
ENABLE_MAIN_ACCOUNT_EXTREME_BUY_WHEN_EMPTY = True  # 空仓且价ATR倍 >= 阈值时全仓买入
MAIN_ACCOUNT_EXTREME_BUY_PRICE_ATR_THRESHOLD = 4.0  # 极度买入价ATR倍阈值
MAIN_ACCOUNT_EXTREME_BUY_CONSECUTIVE_DAYS = 1  # 极度买入需要连续满足的天数

# 追涨买入止损阈值：当追涨买入的仓位跌幅超过该阈值时直接卖出
MAIN_ACCOUNT_UPTREND_STOP_LOSS_PCT = -0.10  # 跌幅超过10%时止损卖出
MAIN_ACCOUNT_UPTREND_STOP_LOSS_ON_MA20 = True  # 追涨买入跌破MA20时清仓
# 低于MA20卖出优化配置
MAIN_ACCOUNT_BELOW_MA20_CONSECUTIVE_DAYS = 3  # 连续低于MA20的天数阈值（默认3天）
MAIN_ACCOUNT_BELOW_MA20_THRESHOLD = -0.04  # 低于MA20的阈值（默认3%）
MAIN_ACCOUNT_BELOW_MA20_EMERGENCY_THRESHOLD = -0.08  # 紧急卖出阈值（默认8%，超过直接卖出）

# 追跌买入止损阈值：当追跌买入的三档都买入后，跌幅超过该阈值时直接卖出
MAIN_ACCOUNT_DROP_STOP_LOSS_PCT = -0.10  # 跌幅超过10%时止损卖出
MAIN_ACCOUNT_DROP_STOP_LOSS_REQUIRE_ALL_LEVELS = True  # 是否需要三档都买入后才触发止损



# 买入A与卖出A之间的止盈机制
ENABLE_MAIN_ACCOUNT_TAKE_PROFIT = True  # 是否启用止盈机制
MAIN_ACCOUNT_TAKE_PROFIT_LEVELS = [0.15, 0.23, 0.30]  # 止盈档位（涨幅10%/20%/30%/40%触发）
MAIN_ACCOUNT_TAKE_PROFIT_RATIOS = [0.20, 0.40, 0.40]  # 对应卖出仓位比例（卖出20%/20%/30%/400%）

# 买入A与卖出A之间的止损机制
ENABLE_MAIN_ACCOUNT_STOP_LOSS = True  # 是否启用止损机制
MAIN_ACCOUNT_STOP_LOSS_LEVELS = [-0.05, -0.10, -0.15]  # 止损档位（跌幅5%/10%/15%触发）
MAIN_ACCOUNT_STOP_LOSS_RATIOS = [0.20, 0.30, 0.50]  # 对应卖出仓位比例（卖出20%/30%/50%）
MAIN_ACCOUNT_STOP_LOSS_REBUY_LEVELS = [-0.10, -0.15, -0.20]  # 再买入档位（跌幅10%/15%/20%买回）



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

    # 计算五日收盘最高价和最低价
    df['5日最高'] = df['收盘'].rolling(window=5, min_periods=1).max()
    df['5日最低'] = df['收盘'].rolling(window=5, min_periods=1).min()
    
    # 计算20日收盘最高价
    df['20日最高'] = df['收盘'].rolling(window=20, min_periods=1).max()

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
    a_take_profit_first_triggered = False
    a_take_profit_prev_distance_to_ma20 = None
    a_take_profit_converge_days = 0
    a_take_profit_extreme_lock_active = False
    a_take_profit_extreme_peak_distance_to_ma20 = None
    a_take_profit_extreme_converge_days = 0
    
    # 买入A与卖出A之间的止损机制状态变量
    stop_loss_levels_triggered = [False] * len(MAIN_ACCOUNT_STOP_LOSS_LEVELS) if ENABLE_MAIN_ACCOUNT_STOP_LOSS else []
    stop_loss_rebuy_levels_triggered = [False] * len(MAIN_ACCOUNT_STOP_LOSS_REBUY_LEVELS) if ENABLE_MAIN_ACCOUNT_STOP_LOSS else []
    
    # 主账户在卖出A与买入A之间的分批买入卖出状态变量
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)  # 主账户卖出A与买入A之间的买入档位
        main_account_sell_buy_position = 0  # 主账户在卖出A与买入A之间的持仓
        main_account_sell_buy_price = 0  # 主账户在卖出A与买入A之间的加权平均买入价格
        main_account_sell_buy_total_shares = 0  # 主账户在卖出A与买入A之间的总买入股数
        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        main_account_uptrend_sell_levels_triggered = [False] * (len(MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS) if ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT else len(MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS))
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

    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'ATR14':>8} {'波动率':>8} {'波幅%':>8} {'价ATR倍':>8} {'极度':>4} {'操作':<30} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * 145)
    
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
            if position > 0 and ENABLE_MAIN_ACCOUNT_TAKE_PROFIT and buy_price > 0:
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
                            take_profit_levels_triggered[0] = False
                    else:
                        a_take_profit_prev_distance_to_ma20 = None
                        a_take_profit_converge_days = 0

                    profit_pct = (close_price - buy_price) / buy_price
                    for tp_idx, tp_level in enumerate(MAIN_ACCOUNT_TAKE_PROFIT_LEVELS):
                        if not take_profit_levels_triggered[tp_idx] and profit_pct >= tp_level:
                            take_profit_triggered = True
                            take_profit_level = tp_idx
                            break
            if position > 0:
                # 卖出条件：波动率>0且降低，且降至前一天97%以下
                if volatility > 0 and is_volatility_declining:
                    volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                    if volatility_ratio <= SELL_RATIO_THRESHOLD:
                        should_sell = True
                        sell_reason = "比率卖出"
            
            # 卖出逻辑
            if position > 0:
                if should_sell:
                    # 立即卖出
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
                        main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        main_account_sell_buy_total_shares = 0
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        main_account_uptrend_sell_levels_triggered = [False] * (len(MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS) if ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT else len(MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS))
                        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                        # 重置追涨/追跌的独立加权平均价格
                        main_account_rise_buy_price = 0
                        main_account_rise_buy_shares = 0
                        main_account_drop_buy_price = 0
                elif take_profit_triggered and take_profit_level >= 0:
                    # 止盈卖出：部分卖出
                    tp_ratio = MAIN_ACCOUNT_TAKE_PROFIT_RATIOS[take_profit_level]
                    sell_shares = int(position * tp_ratio)
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
                        action = f"止盈{take_profit_level + 1}@{sell_price:.2f} 持仓{position_before_sell}→{position}"
                        # 标记该止盈档位已触发
                        take_profit_levels_triggered[take_profit_level] = True
                        a_take_profit_first_triggered = True
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
            
            # 买入逻辑（只有在没有持仓时才买入）
            if position == 0:
                # 条件A买入（全仓）
                if condition_a:
                    buy_price = close_price
                    new_position = int(cash / buy_price / 100) * 100
                    if new_position >= 100:
                        # 如果主账户在卖出A与买入A之间还有持仓，先全部卖出
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and main_account_sell_buy_position > 0:
                            # 先卖出持仓（按当前价格）
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
                            # 记录卖出操作
                            if action == "":
                                action = f"主账户卖出A与买入A之间持仓{main_account_sell_buy_position}股@{buy_price:.2f}"
                            # 重置持仓
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                            main_account_uptrend_sell_levels_triggered = [False] * (len(MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS) if ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT else len(MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS))
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                            # 重新计算可买入股数（100的整数倍）
                            new_position = int(cash / buy_price / 100) * 100
                        
                        position = new_position
                        cost = position * buy_price
                        cash -= cost
                        trade_count += 1
                        actual_condition_a_buy_count += 1
                        a_take_profit_first_triggered = False
                        a_take_profit_prev_distance_to_ma20 = None
                        a_take_profit_converge_days = 0
                        a_take_profit_extreme_lock_active = False
                        a_take_profit_extreme_peak_distance_to_ma20 = None
                        a_take_profit_extreme_converge_days = 0
                        action = f"买入A@{buy_price:.2f}"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': buy_price,
                            'shares': position
                        })
                        volatility_declining_days = 0
                        # 记录持仓开始日期
                        if holding_start_date is None:
                            holding_start_date = date_str
        
        # 主账户在卖出A与买入A之间的分批买入卖出逻辑
        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and position == 0 and (main_account_drop_anchor_price > 0 or main_account_rise_anchor_price > 0):
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
                        rise_levels_str = ','.join([f"\u8ffd\u6da8\u4e70{idx+1}" for idx in executed_rise_levels])
                        # 在买入输出中增加加权价格信息
                        rise_buy_price_info = f" 加权价={main_account_rise_buy_price:.2f}" if main_account_rise_buy_price > 0 else ""
                        action = f"\u4e3b\u8d26\u6237{rise_levels_str}@{close_price:.2f}{rise_buy_price_info} \u6301\u4ed3{main_account_sell_buy_position}"
                        has_buy_today = True
            if close_price < ma20 and drop_anchor_price > 0 and not has_buy_today:
                executed_drop_levels = []
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
                    drop_levels_str = ','.join([f"\u4e70{idx + 1}" for idx in executed_drop_levels])
                    action = f"\u4e3b\u8d26\u6237{drop_levels_str}@{close_price:.2f} \u6301\u4ed3{main_account_sell_buy_position}"
                    has_buy_today = True

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
                        main_account_uptrend_sell_levels_triggered[0] = False
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
                stop_loss_type = None  # 'rise' 或 'drop' 或 'ma20'
                
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

                use_uptrend_profit_sell = (
                    main_account_had_rise_entry_in_cycle
                    and ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT
                    and main_account_rise_buy_price > 0
                )

                can_evaluate_sell = stop_loss_triggered or (
                    (not main_account_extreme_lock_active)
                    and (use_uptrend_profit_sell or current_atr > 0)
                )
                if can_evaluate_sell:
                    price_atr_multiplier = (close_price - main_account_sell_buy_price) / current_atr if current_atr > 0 else 0

                    if main_account_had_rise_entry_in_cycle:
                        if use_uptrend_profit_sell:
                            sell_levels = MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS
                            sell_trigger_value = rise_profit_for_sell
                        else:
                            sell_levels = MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS
                            sell_trigger_value = price_atr_multiplier
                        sell_ratios = MAIN_ACCOUNT_UPTREND_SELL_RATIOS
                    else:
                        sell_levels = MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS
                        sell_trigger_value = price_atr_multiplier
                        sell_ratios = MAIN_ACCOUNT_SELL_RATIOS

                    max_triggered_level = -1
                    newly_triggered_levels = []
                    for sell_idx in range(len(sell_levels) - 1, -1, -1):
                        if sell_trigger_value >= sell_levels[sell_idx]:
                            max_triggered_level = sell_idx
                            break
                    
                    if (not stop_loss_triggered) and main_account_had_rise_entry_in_cycle and max_triggered_level >= 0:
                        trigger_cap = min(max_triggered_level + 1, len(main_account_uptrend_sell_levels_triggered))
                        for idx in range(trigger_cap):
                            if not main_account_uptrend_sell_levels_triggered[idx]:
                                main_account_uptrend_sell_levels_triggered[idx] = True
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
                        else:
                            sell_shares = 0
                    elif main_account_had_rise_entry_in_cycle:
                        if newly_triggered_levels:
                            step_ratio = sum(sell_ratios[idx] for idx in newly_triggered_levels if idx < len(sell_ratios))
                            sell_shares = int(main_account_sell_buy_position * step_ratio)
                            sell_shares = min(sell_shares, main_account_sell_buy_position)
                            trade_level = newly_triggered_levels[-1] + 1
                        else:
                            sell_shares = 0
                    elif max_triggered_level >= 0:
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
                            else:
                                triggered_levels = "止损"
                        else:
                            if main_account_had_rise_entry_in_cycle and newly_triggered_levels:
                                triggered_levels = ",".join([f"\u8ffd\u6da8\u5356{i+1}" for i in newly_triggered_levels])
                            else:
                                sell_source = "\u8ffd\u6da8" if main_account_had_rise_entry_in_cycle else "\u8ffd\u8dcc"
                                triggered_levels = ",".join([f"{sell_source}\u5356{i+1}" for i in range(max_triggered_level + 1)])
                        remaining_position = main_account_sell_buy_position
                        # 在卖出输出中增加加权价格信息
                        rise_buy_price_info = f" 加权价={main_account_rise_buy_price:.2f}" if main_account_rise_buy_price > 0 else ""
                        action = f"\u4e3b\u8d26\u6237{triggered_levels}@{close_price:.2f}{rise_buy_price_info} \u6301\u4ed3{remaining_position}"
                        # 卖出后重置买入档位，允许在更低价格继续追跌买入
                        main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
                            main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                            main_account_uptrend_sell_levels_triggered = [False] * (len(MAIN_ACCOUNT_UPTREND_SELL_PROFIT_LEVELS) if ENABLE_MAIN_ACCOUNT_UPTREND_SELL_BY_PROFIT else len(MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS))
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
                                main_account_sell_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                                main_account_drop_buy_price = 0
                                main_account_drop_anchor_price = close_price  # update drop anchor after sell
                                main_account_extreme_lock_active = False
                                main_account_extreme_peak_price_atr = None
                                main_account_extreme_converge_days = 0
                                main_account_extreme_buy_consecutive_days = 0  # 重置极度买入计数器

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
        
        # 标记极度远离MA20的情况（价ATR倍 >= 4.0）
        price_atr_value = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
        extreme_marker = "★" if price_atr_value >= MAIN_ACCOUNT_TAKE_PROFIT_EXTREME_LOCK_PRICE_ATR else ""

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {atr14_str:>8} {volatility_str:>8} {volatility_pct_str:>8} {price_atr_ratio_str:>8} {extreme_marker:>4} {action:<30} {position_str:>8} {market_value:>12,.2f}")
    
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


