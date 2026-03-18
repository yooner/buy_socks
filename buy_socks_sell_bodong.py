"""
波动率策略 - 基于波动率变化的交易策略
买入：条件A(波动率连续向0靠近)、条件B(波动率从负变正)、条件C(波动率>1连续天数)
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
SELL_RATIO_THRESHOLD = 0.999  # 波动率降至前一天97%以下全卖

# 条件C全局开关
ENABLE_CONDITION_C = False  # 设置为False可关闭条件C买入

# 买入条件全局参数
BUY_DECLINE_DAYS_REQUIRED = 3  # 波动率连续向0靠近所需天数（条件A）

# 条件C参数
BUY_CONDITION_C_DAYS = 5       # 波动率>1的连续天数要求
BUY_CONDITION_C_VOL_THRESHOLD = 1  # 波动率阈值（用于计数）

# 条件C分仓买入参数
CONDITION_C_POSITION_THRESHOLD = 4  # 价ATR倍阈值，超过此值需要分仓买入（默认3）
CONDITION_C_MA20_PCT_THRESHOLD = -20  # MA20幅%阈值，小于此值（如-10%）需要分仓买入（默认-10%）
CONDITION_C_FIRST_POSITION_RATIO = 1/3 # 第一次买入比例（1/3仓）
CONDITION_C_SECOND_POSITION_RATIO = 1/3  # 第二次买入比例（1/3仓）

# 延迟买入开关
ENABLE_DELAYED_BUY = False  # 设置为True启用延迟买入模式）

# 延迟卖出开关
ENABLE_DELAYED_SELL = False  # 设置为True启用延迟卖出模式

# C3分批卖出开关（条件C全仓后跌破20日高价时分批卖出）
ENABLE_C3_PARTIAL_SELL = False  # 设置为True启用C3分批卖出机制
C3_PARTIAL_SELL_RATIO = 1/2  # 每次卖出1/4（可配置）
C3_SELL_STAGES = 2  # 分4次卖完
C_FULL_BUY_HIGH_20_THRESHOLD = 0.97  # 全仓买入C触发分批卖出的20日高价阈值（默认0.97，即满仓价格>=20日最高价*0.97时触发）

# 持仓期间价格追踪止损开关
ENABLE_STOP_LOSS = False  # 设置为True启用持仓期间价格追踪止损策略
STOP_LOSS_MA20_THRESHOLD = -7 # MA20阈值%，价格低于MA20但在阈值范围内不卖出（默认-5%，即低于MA20 5%以内不卖出）

# 买入A五日最高条件开关
ENABLE_BUY_A_5DAY_HIGH_CHECK = False  # 设置为True启用：买入A时如果收盘价是五日最高则延迟买入，等待价格回调后再上涨时买入

# A策略卖出/回补挡位（相对A入场价）
A_SELL_DROP_LEVEL = -0.05   # A卖出触发跌幅（例如 -5%）
A_REBUY_DROP_LEVEL = -0.10  # A回补触发跌幅（例如 -10%）

# A条件波幅变化卖出策略开关
ENABLE_A_VOL_CHANGE_SELL = False  # 设置为True启用：波动率从负变正后，第二天开始监控波幅变化%，由负变正时卖出

# 可买未买资金账户开关
ENABLE_MISSED_BUY_FUND = False  # 设置为True启用：独立资金账户，在买入A与卖出区间内分批买入卖出

# 主账户在卖出A与买入A之间的分批买入卖出开关
ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING = True  # 设置为True启用：主账户在卖出A与买入A之间分批买入卖出

# 可买未买资金分批买入配置（相对于主账户A买入价的跌幅）
MISSED_BUY_LEVELS = [-0.04, -0.08, -0.13]  # 买入触发跌幅（-4%, -8%, -13%, -19%, -26%）
MISSED_BUY_RATIOS = [0.20, 0.30, 0.50]      # 对应买入比例（20%, 30%, 50%）

# 可买未买资金分批卖出配置（相对于买入成本的ATR倍数）
MISSED_SELL_ATR_MULTIPLIERS = [1.0, 1.5, 2.0]  # 卖出触发ATR倍数（1.0, 1.5, 2.0）
MISSED_SELL_RATIOS = [0.30, 0.30, 0.40]          # 对应卖出比例（30%, 30%, 40%）
# 主账户区间交易：剩余仓位小于等于该阈值时直接清仓，避免长期残仓
MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR = 300
# 主账户区间交易：上涨场景分批买入（参考outbreak的趋势确认思路）
ENABLE_MAIN_ACCOUNT_UPTREND_BUY = False
MAIN_ACCOUNT_UPTREND_LEVELS = [0.03, 0.06, 0.10]   # 相对锚定价上涨3%/6%/10%触发
MAIN_ACCOUNT_UPTREND_RATIOS = [0.20, 0.30, 0.50]   # 对应分批投入比例
MAIN_ACCOUNT_UPTREND_REQUIRE_MA20_UP = True         # 要求MA20较前一日上行
# 追涨过滤（防止“只是上涨一点就追”）
MAIN_ACCOUNT_UPTREND_BREAKOUT_LOOKBACK = 10         # 需突破最近N日收盘高点（不含当日）
MAIN_ACCOUNT_UPTREND_BREAKOUT_BUFFER = 0.003        # 突破缓冲(0.3%)，过滤假突破
MAIN_ACCOUNT_UPTREND_MIN_DAYS_AFTER_ANCHOR = 2      # 锚定日后至少等待N天再追涨
MAIN_ACCOUNT_UPTREND_MAX_DISTANCE_TO_MA20 = 0.12    # 收盘价高于MA20超过该比例则不追（防过度追高）
MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS = [1.2, 2.0, 3.0]
MAIN_ACCOUNT_UPTREND_SELL_RATIOS = [0.30, 0.30, 0.40]



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
    missed_buy_groups_count = 0  # 可买未买天数计数（每天单独计算）
    
    # 可买未买资金账户买入缓冲相关变量
    consecutive_missed_days = 0  # 连续可买未买天数计数
    
    # 买入条件计数器
    volatility_declining_days = 0  # 波动率连续向0靠近天数（数值变大）
    prev_volatility = None         # 前一天波动率
    volatility_above_one_days = 0  # 波动率在1以上的连续天数
    is_condition_c_trade = False   # 标记是否为条件C买入的交易
    is_condition_a_trade = False   # 标记是否为A策略买入（用于A挡位卖出/回补）
    
    # 延迟买入状态变量
    pending_buy_price = 0          # 待买入价格（记录触发买入条件时的价格）
    pending_buy_condition = ""     # 待买入条件类型（A/B/C）
    is_pending_buy = False         # 是否有待执行的买入
    pending_buy_volatility = 0     # 待买入时的波动率（用于判断第二天是否继续向0逼近）
    
    # 延迟卖出状态变量
    pending_sell_price = 0         # 待卖出价格（记录触发卖出条件时的价格）
    is_pending_sell = False        # 是否有待执行的卖出
    pending_sell_base_price = 0    # 待卖出建立时的原始价格（用于计算分批卖出盈利百分比）
    
    # 持仓期间价格追踪变量（新的止损策略）
    hold_days = 0                  # 持仓天数计数
    highest_price_since_buy = 0    # 买入后最高价（用于上涨趋势追踪）
    lowest_price_since_buy = 0     # 买入后最低价（用于下跌趋势追踪）
    price_trend_direction = None   # 价格趋势方向: 'up'(上涨), 'down'(下跌), None(未确定)
    
    # 镜像虚拟仓状态变量（用于独立运行原卖出逻辑）
    virtual_position = 0           # 虚拟仓持仓数量（完全镜像实际仓，只是不触发止损）

    # 条件C分仓买入状态变量
    condition_c_position_stage = 0  # 分仓买入阶段：0=未开始, 1=已买第一批, 2=已买第二批, 3=已全仓
    condition_c_prev_price = 0      # 条件C买入前一天的价格（用于判断第二批买入）
    condition_c_prev_ma20_pct = 0   # 条件C买入前一天的MA20幅%（用于判断MA20幅%是否变得更负）
    
    # C条件分批卖出状态变量（适用于所有C条件买入，但仅在满仓后触发）
    c_sell_stage = 0  # C条件分批卖出阶段：0=未开始, 1=已卖第一批, 2=已卖第二批, 3=已卖第三批, 4=已全部卖完
    c_full_buy_price = 0   # C条件满仓买入当天的价格
    c_full_buy_high_20 = 0 # C条件满仓买入当天的20日最高价

    # A条件波幅变化卖出策略状态变量
    a_vol_change_sell_active = False  # 是否激活波幅变化卖出监控（波动率从负变正后第二天开始）
    prev_volatility_change_pct = None  # 前一天的波幅变化%

    # 可买未买资金账户状态变量（独立运营）- 新策略：区间内分批买入卖出
    if ENABLE_MISSED_BUY_FUND:
        missed_buy_fund_initial = initial_capital  # 可买未买资金账户初始资金（每轮重新开始）
        missed_buy_fund_cash = initial_capital  # 可买未买资金账户当前现金
        missed_buy_fund_total_profit = 0  # 可买未买资金账户累计盈利（跨轮累计）
        missed_buy_fund_position = 0           # 可买未买资金账户持仓
        missed_buy_fund_buy_price = 0          # 可买未买资金账户加权平均买入价格
        missed_buy_fund_trades = []            # 可买未买资金账户交易记录
        missed_buy_fund_trade_count = 0        # 可买未买资金账户交易次数
        last_anchor_price = 0
        main_account_anchor_index = -1          # 锚定价格对应的索引（用于追涨等待天数）
        missed_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)  # 记录各买入档位是否已触发
        missed_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)  # 记录各卖出档位是否已触发
        missed_fund_total_bought_shares = 0    # 记录本轮总买入股数（用于计算卖出比例）
        main_account_has_position = False      # 主账户是否有持仓
        is_between_sell_and_buy = False      # 是否在卖出与买入之间（True=卖出后，False=买入后）
    else:
        missed_buy_fund_initial = 0
        missed_buy_fund_total_profit = 0
        missed_buy_fund_cash = 0
        missed_buy_fund_position = 0
        missed_buy_fund_buy_price = 0
        missed_buy_fund_trades = []
        missed_buy_fund_trade_count = 0
        last_anchor_price = 0
        main_account_anchor_index = -1
        missed_buy_levels_triggered = []
        missed_sell_levels_triggered = []
        missed_fund_total_bought_shares = 0
        main_account_has_position = False
        is_between_sell_and_buy = False
    
    # 主账户在卖出A与买入A之间的分批买入卖出状态变量
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)  # 主账户卖出A与买入A之间的买入档位
        main_account_sell_buy_position = 0  # 主账户在卖出A与买入A之间的持仓
        main_account_sell_buy_price = 0  # 主账户在卖出A与买入A之间的加权平均买入价格
        main_account_sell_buy_total_shares = 0  # 主账户在卖出A与买入A之间的总买入股数
        main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)  # 主账户卖出A与买入A之间的卖出档位
        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)  # 主账户卖出A与买入A之间的上涨买入档位
        main_account_drop_anchor_price = 0
        main_account_rise_anchor_price = 0
        main_account_rise_reentry_locked = False
        main_account_had_rise_entry_in_cycle = False
    else:
        main_account_sell_buy_levels_triggered = []
        main_account_sell_buy_position = 0
        main_account_sell_buy_price = 0
        main_account_sell_buy_total_shares = 0
        main_account_sell_sell_levels_triggered = []
        main_account_rise_buy_levels_triggered = []
        main_account_drop_anchor_price = 0
        main_account_rise_anchor_price = 0
        main_account_rise_reentry_locked = False
        main_account_had_rise_entry_in_cycle = False

    # A策略回合状态（用于挡位卖出后等待回补）
    a_cycle_entry_price = 0
    a_cycle_sell_trigger_price = 0
    a_cycle_rebuy_trigger_price = 0
    a_cycle_last_sell_price = 0
    a_cycle_waiting_rebuy = False

    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    def init_a_cycle(entry_price):
        """初始化A策略当前回合：记录入场价并计算卖出/回补挡位"""
        nonlocal a_cycle_entry_price, a_cycle_sell_trigger_price
        nonlocal a_cycle_rebuy_trigger_price, a_cycle_last_sell_price, a_cycle_waiting_rebuy
        a_cycle_entry_price = entry_price
        a_cycle_sell_trigger_price = entry_price * (1 + A_SELL_DROP_LEVEL)
        a_cycle_rebuy_trigger_price = entry_price * (1 + A_REBUY_DROP_LEVEL)
        a_cycle_last_sell_price = 0
        a_cycle_waiting_rebuy = False

    def activate_a_rebuy_wait(sell_price):
        """A挡位卖出后进入等待回补状态，记录本次卖出价"""
        nonlocal a_cycle_last_sell_price, a_cycle_waiting_rebuy
        a_cycle_last_sell_price = sell_price
        a_cycle_waiting_rebuy = True

    def reset_a_cycle():
        """重置A策略回合状态"""
        nonlocal a_cycle_entry_price, a_cycle_sell_trigger_price
        nonlocal a_cycle_rebuy_trigger_price, a_cycle_last_sell_price, a_cycle_waiting_rebuy
        a_cycle_entry_price = 0
        a_cycle_sell_trigger_price = 0
        a_cycle_rebuy_trigger_price = 0
        a_cycle_last_sell_price = 0
        a_cycle_waiting_rebuy = False

    # 打印表头
    log_print(f"\n{'='*175}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"买入条件A: (连续向0靠近{BUY_DECLINE_DAYS_REQUIRED}天且波动率<0) - 全仓买入")
    log_print(f"买入条件C: (波动率>{BUY_CONDITION_C_VOL_THRESHOLD}连续第{BUY_CONDITION_C_DAYS}天) - 分仓买入(价ATR倍>{CONDITION_C_POSITION_THRESHOLD}或MA20幅%<{CONDITION_C_MA20_PCT_THRESHOLD}%时分3批)")
    stop_loss_str = "; 持仓价格追踪止损" if ENABLE_STOP_LOSS else ""
    vol_change_sell_str = "; A条件波幅变化卖出" if ENABLE_A_VOL_CHANGE_SELL else ""
    log_print(f"卖出条件: 波动率>0且降低时，降至前一天{SELL_RATIO_THRESHOLD*100:.0f}%以下则全卖；条件C买入需等>{BUY_CONDITION_C_VOL_THRESHOLD}天数归0才卖{stop_loss_str}{vol_change_sell_str}")
    if ENABLE_C3_PARTIAL_SELL:
        log_print(f"C条件分批卖出: 全仓后跌破20日高价且满仓价>={int(C_FULL_BUY_HIGH_20_THRESHOLD*100)}%20日高时分{C3_SELL_STAGES}批卖出，每次卖出{int(C3_PARTIAL_SELL_RATIO*100)}%（最低优先级）")
    if ENABLE_STOP_LOSS:
        log_print(f"持仓止损: 买入后第二天收盘价<MA20时启动价格追踪，低于MA20 {abs(STOP_LOSS_MA20_THRESHOLD)}%才卖出，防止震荡")
        log_print(f"C条件价格追踪: 买入C后第二天价格<MA20时不启动价格追踪（仅A条件买入启动价格追踪）")
    if ENABLE_A_VOL_CHANGE_SELL:
        log_print(f"A条件波幅变化卖出: 波动率从负变正后，第二天开始监控波幅变化%，由负变正时卖出")
    if ENABLE_MISSED_BUY_FUND:
        log_print(f"可买未买资金账户: 独立资金{initial_capital:,.2f}，买入区间分批买入卖出，最终与主账户同时卖出")
    log_print(f"{'='*175}\n")

    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'MA20幅%':>8} {'ATR14':>8} {'波动率':>8} {'波幅%':>8} {'波幅变化%':>10} {'价ATR倍':>8} {'5日最高':>8} {'5日最低':>8} {'20日最高':>8} {'>1天数':>6} {'操作':<12} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * 195)
    
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
        a_sold_today = False
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
            if volatility > BUY_CONDITION_C_VOL_THRESHOLD:
                volatility_above_one_days += 1
            else:
                volatility_above_one_days = 0

            # 卖出策略
            should_sell = False
            sell_reason = ""
            is_stop_loss_triggered = False  # 标记是否触发新的价格追踪止损
            
            # 新的持仓期间价格追踪止损策略（开关打开时启用）
            if ENABLE_STOP_LOSS and position > 0:
                hold_days += 1
                
                # 买入后第二天开始检查
                if hold_days >= 2:
                    # 买入后第二天，检查是否是第一天站上MA20（前一天<MA20，当天>MA20）
                    # 或者已经跌破MA20，启动价格追踪
                    if hold_days == 2 and highest_price_since_buy == 0 and lowest_price_since_buy == 0:
                        prev_day_ma20 = df.iloc[i-1]['MA20'] if i > 0 and 'MA20' in df.columns else ma20
                        prev_day_close = df.iloc[i-1]['收盘'] if i > 0 else close_price
                        
                        # 条件1：当天收盘价 < MA20（跌破MA20）
                        # 条件2：前一天<MA20且当天>MA20（第一天站上MA20）
                        is_below_ma20 = close_price < ma20
                        is_first_day_above_ma20 = (prev_day_close < prev_day_ma20) and (close_price > ma20)
                        
                        # C条件买入特殊处理：如果价格低于MA20，不启动价格追踪
                        skip_price_track = False
                        if is_condition_c_trade and is_below_ma20:
                            skip_price_track = True
                        
                        if (is_below_ma20 or is_first_day_above_ma20) and not skip_price_track:
                            # 初始化价格追踪
                            highest_price_since_buy = close_price
                            lowest_price_since_buy = close_price
                            price_trend_direction = None
                    
                    # 如果已经在价格追踪模式
                    if highest_price_since_buy > 0 and lowest_price_since_buy > 0:
                        prev_day_close = df.iloc[i-1]['收盘'] if i > 0 else close_price

                        # 使用与DataFrame中相同的MA20幅度%计算方式
                        # MA20幅度% = (MA20 - 收盘) / 收盘 * 100
                        ma20_pct_from_df = row['MA20幅度%']

                        # 价格追踪期间，如果跌破MA20且低于阈值，触发卖出
                        # 注意：STOP_LOSS_MA20_THRESHOLD是负数（如-8），表示低于MA20的百分比
                        if pd.notna(ma20_pct_from_df) and ma20_pct_from_df < STOP_LOSS_MA20_THRESHOLD:
                            is_stop_loss_triggered = True
                            sell_reason = f"跌破MA20阈值({ma20_pct_from_df:.1f}%)"
                        else:
                            # 未跌破MA20，继续判断趋势方向
                            if price_trend_direction is None:
                                # 首次确定趋势方向
                                if close_price > prev_day_close:
                                    price_trend_direction = 'up'
                                    highest_price_since_buy = close_price
                                elif close_price < prev_day_close:
                                    price_trend_direction = 'down'
                                    lowest_price_since_buy = close_price
                                    # 下跌趋势：第一天就卖出
                                    is_stop_loss_triggered = True
                                    sell_reason = "趋势下跌"
                            elif price_trend_direction == 'up':
                                # 上涨趋势：更新最高价，检查是否跌破最高价
                                if close_price > highest_price_since_buy:
                                    highest_price_since_buy = close_price
                                elif close_price < highest_price_since_buy:
                                    # 跌破最高价，触发卖出
                                    is_stop_loss_triggered = True
                                    sell_reason = "趋势上涨破高"
                            elif price_trend_direction == 'down':
                                # 下跌趋势：更新最低价
                                if close_price < lowest_price_since_buy:
                                    lowest_price_since_buy = close_price
                                    # 继续下跌，保持持仓等待虚拟仓卖出
            
            # 其他卖出条件（仅在未触发价格追踪止损时检查）
            # 注意：条件C买入的交易也支持价格追踪止损，优先级最高
            # C条件分批卖出逻辑：C条件满仓后跌破20日高价时分批卖出（适用于所有C条件买入的持仓）
            # 增加限定条件：C条件满仓买入当天价格 >= 20日最高价 * C_FULL_BUY_HIGH_20_THRESHOLD（默认0.97）
            # 优先级：C条件分批卖出 > C条件卖出（分批卖出优先）
            c_partial_sell_triggered = False
            if ENABLE_C3_PARTIAL_SELL and is_condition_c_trade and position > 0 and condition_c_position_stage == 3:
                high_20 = row['20日最高'] if pd.notna(row['20日最高']) else 0
                # 检查C条件满仓买入当天是否满足条件：买入价格 >= 20日最高价 * 阈值（默认0.97）
                c_full_buy_met_condition = c_full_buy_price >= c_full_buy_high_20 * C_FULL_BUY_HIGH_20_THRESHOLD if c_full_buy_price > 0 and c_full_buy_high_20 > 0 else False
                if c_full_buy_met_condition and close_price < high_20 and c_sell_stage < C3_SELL_STAGES:
                    c_partial_sell_triggered = True
            
            # C条件卖出检查（仅在未触发C条件分批卖出时检查）
            if not is_stop_loss_triggered and not c_partial_sell_triggered:
                if is_condition_c_trade and position > 0:
                    # 条件C买入的交易：只要volatility_above_one_days归0就卖出（不判断波动率是否降低）
                    if volatility_above_one_days == 0:
                        should_sell = True
                        sell_reason = "C条件"
                elif position > 0 and not is_condition_c_trade:
                    # A策略优先卖出：价格跌破A卖出挡位（如-5%）即卖出
                    if is_condition_a_trade and a_cycle_sell_trigger_price > 0 and close_price <= a_cycle_sell_trigger_price:
                        should_sell = True
                        sell_reason = "A挡位卖出"

                    # 普通卖出条件1：波动率>0且降低，且降至前一天97%以下
                    if not should_sell and volatility > 0 and is_volatility_declining:
                        volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                        if volatility_ratio <= SELL_RATIO_THRESHOLD:
                            should_sell = True
                            sell_reason = "比率卖出"
                    
                    # A条件波幅变化卖出策略：波动率从负变正后，第二天开始监控波幅变化%，由负变正时卖出
                    if ENABLE_A_VOL_CHANGE_SELL and not should_sell:
                        # 检查波动率是否从负变正（当天为正，前一天为负）
                        # 注意：当天检测到从负变正时，只是标记，第二天才开始真正监控
                        if prev_volatility is not None and volatility > 0 and prev_volatility < 0:
                            # 波动率从负变正，设置标记（第二天才开始监控）
                            a_vol_change_sell_active = True
                        
                        # 如果监控已激活，且波动率已经是正数（说明已经过了从负变正的那一天）
                        if a_vol_change_sell_active and volatility > 0 and prev_volatility_change_pct is not None:
                            # 波幅变化%由负变正时触发卖出
                            if volatility_change_pct > 0 and prev_volatility_change_pct < 0:
                                # 波幅变化%由负变正，触发卖出
                                should_sell = True
                                sell_reason = "A波幅变化"
                    
            
            # 卖出逻辑（支持延迟卖出和立即止损）
            if position > 0:
                # 止损卖出：立即执行，不进入待卖出状态
                if is_stop_loss_triggered:
                    was_condition_c_trade = is_condition_c_trade
                    was_condition_a_trade = is_condition_a_trade
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
                    is_condition_c_trade = False
                    is_condition_a_trade = False
                    # 重置计数器
                    volatility_declining_days = 0
                    volatility_above_one_days = 0  # 卖出后重置C条件天数
                    # 重置条件C分仓状态
                    condition_c_position_stage = 0
                    condition_c_prev_price = 0
                    condition_c_prev_ma20_pct = 0
                    # 重置C条件分批卖出状态
                    c_sell_stage = 0
                    c_full_buy_price = 0
                    c_full_buy_high_20 = 0
                    # 重置延迟状态
                    is_pending_buy = False
                    pending_buy_price = 0
                    pending_buy_condition = ""
                    is_pending_sell = False
                    pending_sell_price = 0
                    pending_sell_base_price = 0  # 重置待卖出原始价格
                    # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                    hold_days = 0
                    # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                    price_trend_direction = None
                    
                    # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                        # 设置锚定价格为卖出价格
                        last_anchor_price = sell_price
                        main_account_drop_anchor_price = sell_price
                        main_account_rise_anchor_price = sell_price
                        main_account_rise_reentry_locked = False
                        main_account_had_rise_entry_in_cycle = False
                        # 重置主账户在卖出A与买入A之间的分批买入卖出状态变量
                        main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                        main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        main_account_sell_buy_total_shares = 0
                        main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                        main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                    # 重置A条件波幅变化卖出策略状态
                    a_vol_change_sell_active = False
                    prev_volatility_change_pct = None
                    if was_condition_a_trade and sell_reason == "A挡位卖出":
                        activate_a_rebuy_wait(sell_price)
                        a_sold_today = True
                    else:
                        reset_a_cycle()
                    # 可买未买资金账户同步卖出
                    if ENABLE_MISSED_BUY_FUND and missed_buy_fund_position > 0:
                        missed_buy_fund_sell_value = missed_buy_fund_position * sell_price
                        missed_buy_fund_profit = (sell_price - missed_buy_fund_buy_price) * missed_buy_fund_position
                        missed_buy_fund_cash += missed_buy_fund_sell_value
                        missed_buy_fund_trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '卖出',
                            'price': sell_price,
                            'shares': missed_buy_fund_position,
                            'profit': missed_buy_fund_profit
                        })
                        missed_buy_fund_position = 0
                        missed_buy_fund_buy_price = 0
                        last_main_buy_price = 0
                        missed_fund_total_bought_shares = 0  # 重置总买入股数
                
                # 检查是否有待执行的延迟卖出（价格追踪止损不进入此逻辑）
                elif is_pending_sell and ENABLE_DELAYED_SELL:
                    # 如果当天价格高于待卖出价，更新卖出价（取更高的价格）
                    if close_price > pending_sell_price:
                        pending_sell_price = close_price
                        action = f"更新卖价@{pending_sell_price:.2f}"
                    # 如果收盘价低于待卖出价，执行卖出
                    elif close_price <= pending_sell_price:
                        was_condition_c_trade = is_condition_c_trade
                        was_condition_a_trade = is_condition_a_trade
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
                        is_condition_c_trade = False
                        is_condition_a_trade = False
                        # 重置计数器
                        volatility_declining_days = 0
                        volatility_above_one_days = 0  # 卖出后重置C条件天数
                        # 重置条件C分仓状态
                        condition_c_position_stage = 0
                        condition_c_prev_price = 0
                        # 重置C条件分批卖出状态
                        c_sell_stage = 0
                        c_full_buy_price = 0
                        c_full_buy_high_20 = 0
                        # 重置延迟状态
                        is_pending_buy = False
                        pending_buy_price = 0
                        pending_buy_condition = ""
                        is_pending_sell = False
                        pending_sell_price = 0
                        pending_sell_base_price = 0
                        # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                        hold_days = 0
                        # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                        price_trend_direction = None
                        # 重置A条件波幅变化卖出策略状态
                        a_vol_change_sell_active = False
                        prev_volatility_change_pct = None
                        if was_condition_a_trade and sell_reason == "A挡位卖出":
                            activate_a_rebuy_wait(sell_price)
                            a_sold_today = True
                        else:
                            reset_a_cycle()
                        
                        # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                            # 设置锚定价格为卖出价格
                            last_anchor_price = sell_price
                            main_account_anchor_index = i
                            main_account_drop_anchor_price = sell_price
                            main_account_rise_anchor_price = sell_price
                            main_account_rise_reentry_locked = False
                            main_account_had_rise_entry_in_cycle = False
                            # 重置主账户在卖出A与买入A之间的分批买入卖出状态变量
                            main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
                            main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                        # 非止损卖出，同步清空虚拟仓
                        virtual_position = 0
                
                # 正常卖出逻辑（非延迟模式或触发卖出条件时）
                elif should_sell:
                    # C条件卖出立即执行，不受延迟卖出开关影响（最高优先级）
                    # 普通卖出（比率卖出）受延迟卖出开关控制
                    is_condition_c_sell = (sell_reason == "C条件")
                    is_condition_a_sell = (sell_reason == "A挡位卖出")
                    if ENABLE_DELAYED_SELL and not is_pending_sell and not is_condition_c_sell and not is_condition_a_sell:
                        # 延迟卖出模式：记录卖出点但不真正卖出（仅对普通卖出生效）
                        is_pending_sell = True
                        pending_sell_price = close_price
                        pending_sell_base_price = close_price  # 保存待卖出建立时的原始价格
                        action = f"待卖出@{pending_sell_price:.2f}"
                    else:
                        # 正常模式或已有待卖出：立即卖出
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
                        was_condition_a_trade = is_condition_a_trade
                        is_condition_c_trade = False
                        is_condition_a_trade = False
                        # 卖出后重置计数器
                        volatility_declining_days = 0
                        volatility_above_one_days = 0  # 卖出后重置C条件天数
                        # 重置条件C分仓状态
                        condition_c_position_stage = 0
                        condition_c_prev_price = 0
                        # 重置C条件分批卖出状态
                        c_sell_stage = 0
                        c_full_buy_price = 0
                        c_full_buy_high_20 = 0
                        # 重置延迟买入状态
                        is_pending_buy = False
                        pending_buy_price = 0
                        pending_buy_condition = ""
                        # 重置延迟卖出状态
                        is_pending_sell = False
                        pending_sell_price = 0
                        pending_sell_base_price = 0
                        # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                        hold_days = 0
                        # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                        price_trend_direction = None
                        if was_condition_a_trade and sell_reason == "A挡位卖出":
                            activate_a_rebuy_wait(sell_price)
                            a_sold_today = True
                        else:
                            reset_a_cycle()

                        # 非止损卖出，同步清空虚拟仓
                        virtual_position = 0
                        
                        # 可买未买资金账户处理：主账户卖出时
                        if ENABLE_MISSED_BUY_FUND:
                            # 无论是否有持仓，都设置锚定价格为卖出价格，并标记为卖出与买入之间
                            last_anchor_price = sell_price
                            main_account_anchor_index = i
                            main_account_drop_anchor_price = sell_price
                            main_account_rise_anchor_price = sell_price
                            main_account_rise_reentry_locked = False
                            main_account_had_rise_entry_in_cycle = False
                            main_account_has_position = False
                            is_between_sell_and_buy = True
                        
                        # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                            # 设置锚定价格为卖出价格
                            last_anchor_price = sell_price
                            main_account_anchor_index = i
                            main_account_drop_anchor_price = sell_price
                            main_account_rise_anchor_price = sell_price
                            main_account_rise_reentry_locked = False
                            main_account_had_rise_entry_in_cycle = False
                            # 重置主账户在卖出A与买入A之间的分批买入卖出状态变量
                            main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            main_account_sell_buy_total_shares = 0
                            main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                            main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                        # 如果有持仓则同步清空
                        if ENABLE_MISSED_BUY_FUND and missed_buy_fund_position > 0:
                                missed_buy_fund_sell_value = missed_buy_fund_position * sell_price
                                missed_buy_fund_profit = (sell_price - missed_buy_fund_buy_price) * missed_buy_fund_position
                                missed_buy_fund_cash += missed_buy_fund_sell_value
                                missed_buy_fund_trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '卖出',
                                    'price': sell_price,
                                    'shares': missed_buy_fund_position,
                                    'profit': missed_buy_fund_profit
                                })
                                missed_buy_fund_position = 0
                                missed_buy_fund_buy_price = 0
                                missed_fund_total_bought_shares = 0
                                missed_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                                missed_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                
                # C条件分批卖出逻辑（优先级高于C条件卖出，但低于止损和延迟卖出）
                # 当C条件分批卖出触发时，优先执行分批卖出而不是C条件卖出
                elif c_partial_sell_triggered and position > 0:
                    # 计算本次卖出的股数
                    sell_shares = int(position * C3_PARTIAL_SELL_RATIO)
                    if sell_shares > 0:
                        sell_price = close_price
                        sell_value = sell_shares * sell_price
                        profit = (sell_price - buy_price) * sell_shares
                        cash += sell_value
                        position -= sell_shares
                        c_sell_stage += 1
                        
                        action = f"卖出C-{c_sell_stage}@{sell_price:.2f}(破20日高)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '卖出',
                            'price': sell_price,
                            'shares': sell_shares,
                            'profit': profit
                        })
                        
                        # 如果全部卖出，重置所有状态
                        if position <= 0:
                            position = 0
                            buy_price = 0
                            is_condition_c_trade = False
                            volatility_declining_days = 0
                            volatility_above_one_days = 0
                            condition_c_position_stage = 0
                            condition_c_prev_price = 0
                            condition_c_prev_ma20_pct = 0
                            c_sell_stage = 0
                            c_full_buy_price = 0
                            c_full_buy_high_20 = 0
                            is_pending_buy = False
                            pending_buy_price = 0
                            pending_buy_condition = ""
                            is_pending_sell = False
                            pending_sell_price = 0
                            pending_sell_base_price = 0
                            hold_days = 0
                            price_trend_direction = None
                            highest_price_since_buy = 0
                            lowest_price_since_buy = 0
                            virtual_position = 0
                            
                            # 可买未买资金账户同步卖出（C条件分批卖出全部完成时）
                            if ENABLE_MISSED_BUY_FUND and missed_buy_fund_position > 0:
                                missed_buy_fund_sell_value = missed_buy_fund_position * sell_price
                                missed_buy_fund_profit = (sell_price - missed_buy_fund_buy_price) * missed_buy_fund_position
                                missed_buy_fund_cash += missed_buy_fund_sell_value
                                missed_buy_fund_trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '卖出',
                                    'price': sell_price,
                                    'shares': missed_buy_fund_position,
                                    'profit': missed_buy_fund_profit
                                })
                                missed_buy_fund_position = 0
                                missed_buy_fund_buy_price = 0
                                last_main_buy_price = 0
                                missed_fund_total_bought_shares = 0  # 重置总买入股数
            
            # 虚拟仓独立运行原卖出逻辑（不触发止损，使用实际仓的状态）
            if ENABLE_STOP_LOSS and virtual_position > 0 and position == 0:
                virtual_should_sell = False
                
                # 虚拟仓只使用原卖出逻辑（不检查止损），使用实际仓的is_condition_c_trade状态
                if is_condition_c_trade:
                    # 条件C买入的交易：只要volatility_above_one_days归0就卖出
                    if volatility_above_one_days == 0:
                        virtual_should_sell = True
                else:
                    # 普通卖出条件1：波动率>0且降低，且降至前一天97%以下
                    if volatility > 0 and is_volatility_declining:
                        volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                        if volatility_ratio <= SELL_RATIO_THRESHOLD:
                            virtual_should_sell = True
                
                # 虚拟仓卖出时，重置所有计数器
                if virtual_should_sell:
                    virtual_position = 0
                    # 重置所有计数器（等同于实际卖出后的重置）
                    volatility_declining_days = 0
                    volatility_above_one_days = 0  # 重置C条件天数
                    # 重置条件C标记
                    is_condition_c_trade = False
                    # 重置条件C分仓状态
                    condition_c_position_stage = 0
                    condition_c_prev_price = 0
                    # 重置C条件分批卖出状态
                    c_sell_stage = 0
                    c_full_buy_price = 0
                    c_full_buy_high_20 = 0

            # 更新前一天的波动率
            prev_volatility = volatility
            
            # 更新前一天的波幅变化%（用于A条件波幅变化卖出策略）
            if pd.notna(volatility_change_pct):
                prev_volatility_change_pct = volatility_change_pct

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
            
            # 条件C：波动率>阈值连续指定天数（仅在开关打开时启用）
            condition_c = ENABLE_CONDITION_C and volatility_above_one_days >= BUY_CONDITION_C_DAYS
            
            # 条件C分仓继续买入逻辑（在已有持仓且未全仓时）
            if position > 0 and is_condition_c_trade and condition_c_position_stage in [1, 2]:
                buy_price = close_price
                current_ma20_pct = row['MA20幅度%'] if pd.notna(row['MA20幅度%']) else 0
                
                # 判断是否可以加仓的条件：
                # 1. 价格 > 前一天价格
                # 2. MA20幅% 没有变得更负（即 current_ma20_pct >= condition_c_prev_ma20_pct）
                price_increasing = close_price > condition_c_prev_price
                ma20_pct_not_worsening = current_ma20_pct >= condition_c_prev_ma20_pct  # MA20幅%没有变得更负
                
                if condition_c_position_stage == 1:
                    # 判断是否可以买入C2
                    if price_increasing and ma20_pct_not_worsening:
                        # 第二批买入 1/3
                        new_position = int(cash * CONDITION_C_SECOND_POSITION_RATIO / buy_price)
                        if new_position > 0:
                            additional_position = new_position
                            cost = additional_position * buy_price
                            cash -= cost
                            position += additional_position
                            trade_count += 1
                            condition_c_position_stage = 2
                            condition_c_prev_price = buy_price
                            condition_c_prev_ma20_pct = current_ma20_pct
                            action = f"买入C2@{buy_price:.2f}(涨,幅{current_ma20_pct:.1f}%)"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'shares': additional_position,
                                'is_condition_c': True
                            })
                    elif price_increasing and not ma20_pct_not_worsening:
                        # 价格上涨但MA20幅%变得更负，更新参考价格但不买入
                        condition_c_prev_price = buy_price
                        condition_c_prev_ma20_pct = current_ma20_pct
                        action = f"更新C参考@{buy_price:.2f}(幅{current_ma20_pct:.1f}%)"
                    # 如果价格没有上涨，不更新参考价格，保持当前持仓
                elif condition_c_position_stage == 2:
                    # 第三批买入，满仓（同样需要价格上涨且MA20幅%没有变得更负）
                    if price_increasing and ma20_pct_not_worsening:
                        new_position = int(cash / buy_price)
                        if new_position > 0:
                            additional_position = new_position
                            cost = additional_position * buy_price
                            cash -= cost
                            position += additional_position
                            trade_count += 1
                            condition_c_position_stage = 3
                            condition_c_prev_price = buy_price
                            condition_c_prev_ma20_pct = current_ma20_pct
                            # 记录C条件满仓买入当天的价格和20日最高价（用于分批卖出条件判断）
                            c_full_buy_price = buy_price
                            c_full_buy_high_20 = row['20日最高'] if pd.notna(row['20日最高']) else 0
                            action = f"买入C3@{buy_price:.2f}(满仓,幅{current_ma20_pct:.1f}%)"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'shares': additional_position,
                                'is_condition_c': True
                            })
                    elif price_increasing and not ma20_pct_not_worsening:
                        # 价格上涨但MA20幅%变得更负，更新参考价格但不买入
                        condition_c_prev_price = buy_price
                        condition_c_prev_ma20_pct = current_ma20_pct
                        action = f"更新C参考@{buy_price:.2f}(幅{current_ma20_pct:.1f}%)"
                    # 如果价格没有上涨，不更新参考价格，保持当前持仓
            
            # 价格追踪期间的延迟买入触发逻辑（只在价格追踪期间且开关打开时启用）
            # 注意：价格追踪期间的买入不需要波动率条件，只需要价格条件
            if position == 0 and ENABLE_DELAYED_BUY and highest_price_since_buy > 0:
                # 价格条件：价格从低点反弹（当天价格 > 前一天价格）
                prev_day_close = df.iloc[i-1]['收盘'] if i > 0 else close_price
                price_rebounding = close_price > prev_day_close
                
                if price_rebounding and not is_pending_buy:
                    # 价格反弹，标记待买入
                    is_pending_buy = True
                    pending_buy_price = close_price
                    pending_buy_condition = "追踪期"  # 标记为价格追踪期间的延迟买入
                    pending_buy_prev_price = prev_day_close  # 记录前一天价格用于比较
                    action = f"待买入@{pending_buy_price:.2f}(追踪期,反弹)"
            
            # 买入逻辑（只有在没有持仓时才买入）
            if position == 0:
                a_rebuy_blocking = False
                # A策略卖出后的回补优先级最高：先看回补挡位，再看是否突破卖出价
                if a_cycle_waiting_rebuy:
                    should_rebuy_by_drop = (not a_sold_today) and a_cycle_rebuy_trigger_price > 0 and close_price <= a_cycle_rebuy_trigger_price
                    should_rebuy_by_breakout = (not a_sold_today) and a_cycle_last_sell_price > 0 and close_price > a_cycle_last_sell_price
                    if should_rebuy_by_drop or should_rebuy_by_breakout:
                        buy_price = close_price
                        new_position = int(cash / buy_price)
                        if new_position > 0:
                            position = new_position
                            cost = position * buy_price
                            cash -= cost
                            trade_count += 1
                            is_condition_c_trade = False
                            is_condition_a_trade = True
                            action_suffix = "回补挡位" if should_rebuy_by_drop else "破卖价回补"
                            action = f"回补A@{buy_price:.2f}({action_suffix})"
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': buy_price,
                                'shares': position,
                                'is_condition_c': False
                            })
                            init_a_cycle(buy_price)
                            volatility_declining_days = 0
                            hold_days = 0
                            highest_price_since_buy = 0
                            lowest_price_since_buy = 0
                            price_trend_direction = None
                            virtual_position = position
                    else:
                        a_rebuy_blocking = True
                        action = f"等待A回补@{close_price:.2f}"

                # 检查是否有待执行的延迟买入
                if a_rebuy_blocking:
                    pass
                elif is_pending_buy:
                    # 判断是五日最高延迟买入还是价格追踪期间延迟买入
                    is_5day_high_pending = "五日最高" in pending_buy_condition
                    
                    # 延迟买入通用逻辑：检查价格是否继续上涨
                    # 当天价格 > 待买入记录时的价格
                    price_continuing_up = close_price > pending_buy_price
                    
                    if price_continuing_up:
                        # 检查买入当天是否是5日高点
                        high_5day = row['5日最高'] if pd.notna(row['5日最高']) else 0
                        low_5day = row['5日最低'] if pd.notna(row['5日最低']) else 0
                        
                        # 判断是否是"从五日低点变成五日高点"
                        # 条件：当天是5日高点，且前一天是5日低点（或非常接近）
                        prev_day_low_5day = df.iloc[i-1]['5日最低'] if i > 0 else low_5day
                        is_from_low_to_high = (close_price >= high_5day and 
                                               prev_day_low_5day <= df.iloc[i-1]['5日最高'] * 1.02)  # 前一天接近5日低点
                        
                        if ENABLE_BUY_A_5DAY_HIGH_CHECK and close_price >= high_5day and not is_from_low_to_high:
                            # 当天是5日高点，但不是从低点涨上来的，继续延迟买入，更新待买入价格
                            pending_buy_price = close_price
                            action = f"待买入A@{pending_buy_price:.2f}(五日最高,续涨)"
                        else:
                            # 价格继续上涨且不是5日高点，或者是从五日低点变成五日高点，执行买入
                            buy_price = close_price
                            new_position = int(cash / buy_price)
                            if new_position > 0:
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                # 如果是五日最高延迟买入成功执行，计入A买入次数
                                if is_5day_high_pending:
                                    actual_condition_a_buy_count += 1
                                is_condition_c_trade = False
                                is_condition_a_trade = is_5day_high_pending
                                if is_from_low_to_high:
                                    action = f"买入A@{buy_price:.2f}(低转高,续涨)"
                                elif is_5day_high_pending:
                                    action = f"买入A@{buy_price:.2f}(五日高,续涨)"
                                else:
                                    action = f"买入@{buy_price:.2f}(追踪期,续涨)"
                                trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '买入',
                                    'price': buy_price,
                                    'shares': position,
                                    'is_condition_c': False
                                })
                                volatility_declining_days = 0
                                # 重置延迟买入状态
                                is_pending_buy = False
                                pending_buy_price = 0
                                pending_buy_condition = ""
                                pending_buy_prev_price = 0
                                # 初始化持仓期间价格追踪变量
                                hold_days = 0
                                highest_price_since_buy = 0
                                lowest_price_since_buy = 0
                                price_trend_direction = None
                                # 同步更新虚拟仓
                                virtual_position = position
                                if is_condition_a_trade:
                                    init_a_cycle(buy_price)
                                if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                                    main_account_drop_anchor_price = 0
                                    main_account_rise_anchor_price = 0
                                    main_account_rise_reentry_locked = False
                                    main_account_had_rise_entry_in_cycle = False
                                else:
                                    reset_a_cycle()
                    else:
                        # 价格没有继续上涨，更新待买入价格（取更低的价格）
                        if close_price < pending_buy_price:
                            pending_buy_price = close_price
                            if is_5day_high_pending:
                                action = f"更新待买A@{pending_buy_price:.2f}(五日高,更低)"
                            else:
                                action = f"更新待买@{pending_buy_price:.2f}(追踪期,更低)"
                        # 如果价格持平或上涨但未达到买入条件，保持待买入状态

                # 正常买入逻辑（无待买入时）
                elif not is_pending_buy:
                    # 条件C买入（支持分仓）
                    if condition_c:
                        buy_price = close_price
                        price_atr_ratio = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
                        ma20_pct = row['MA20幅度%'] if pd.notna(row['MA20幅度%']) else 0
                        
                        # 判断触发条件（用于显示）
                        trigger_by_atr = price_atr_ratio > CONDITION_C_POSITION_THRESHOLD
                        trigger_by_ma20 = ma20_pct < CONDITION_C_MA20_PCT_THRESHOLD
                        trigger_type = "倍" if trigger_by_atr else "幅"
                        trigger_value = price_atr_ratio if trigger_by_atr else ma20_pct
                        
                        # 分仓买入条件：价ATR倍 > 阈值 或 MA20幅% < 阈值（或的关系） 或 价格在MA20以上（ma20_pct > 0）
                        need_position_buy = (price_atr_ratio > CONDITION_C_POSITION_THRESHOLD or 
                                            ma20_pct < CONDITION_C_MA20_PCT_THRESHOLD or
                                            ma20_pct > 0)
                        
                        if need_position_buy:
                            # 第一批买入 1/3
                            new_position = int(cash * CONDITION_C_FIRST_POSITION_RATIO / buy_price)
                            if new_position > 0:
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                is_condition_c_trade = True
                                is_condition_a_trade = False
                                condition_c_position_stage = 1
                                condition_c_prev_price = buy_price
                                condition_c_prev_ma20_pct = ma20_pct  # 记录初始MA20幅%
                                action = f"买入C1@{buy_price:.2f}({trigger_type}{trigger_value:.1f})"
                                trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '买入',
                                    'price': buy_price,
                                    'shares': position,
                                    'is_condition_c': True
                                })
                        else:
                            # 不满足分仓条件，全仓买入
                            new_position = int(cash / buy_price)
                            if new_position > 0:
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                is_condition_c_trade = True
                                is_condition_a_trade = False
                                condition_c_position_stage = 3  # 标记为已全仓
                                # 记录C条件满仓买入当天的价格和20日最高价（用于分批卖出条件判断）
                                c_full_buy_price = buy_price
                                c_full_buy_high_20 = row['20日最高'] if pd.notna(row['20日最高']) else 0
                                action = f"买入C@{buy_price:.2f}(全仓)"
                                trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '买入',
                                    'price': buy_price,
                                    'shares': position,
                                    'is_condition_c': True
                                })

                        # 初始化持仓期间价格追踪变量
                        if position > 0 and hold_days == 0:
                            hold_days = 0
                            highest_price_since_buy = 0
                            lowest_price_since_buy = 0
                            price_trend_direction = None
                            # 同步更新虚拟仓（完全镜像）
                            virtual_position = position
                            reset_a_cycle()

                    # 条件A买入（全仓）- 主逻辑立即买入，不延迟
                    elif condition_a:
                        # 检查：如果启用五日最高条件检查，且买入当天收盘价是五日最高，则使用延迟买入机制
                        should_delay_buy = False
                        if ENABLE_BUY_A_5DAY_HIGH_CHECK:
                            high_5day = row['5日最高'] if pd.notna(row['5日最高']) else 0
                            if close_price >= high_5day:
                                should_delay_buy = True
                        
                        if should_delay_buy:
                            # 延迟买入：记录买入点，等待价格回调后再上涨时买入
                            is_pending_buy = True
                            pending_buy_price = close_price
                            pending_buy_condition = "A(五日最高)"
                            action = f"待买入A@{close_price:.2f}(五日最高)"
                        else:
                            buy_price = close_price
                            new_position = int(cash / buy_price)
                            if new_position > 0:
                                # 如果未买资金还有持仓（上一轮未触发卖出），先全部卖出
                                if ENABLE_MISSED_BUY_FUND and missed_buy_fund_position > 0:
                                    # 先卖出持仓（按当前价格）
                                    sell_value = missed_buy_fund_position * buy_price
                                    sell_profit = (buy_price - missed_buy_fund_buy_price) * missed_buy_fund_position
                                    missed_buy_fund_cash += sell_value
                                    missed_buy_fund_trades.append({
                                        'day': day_num,
                                        'date': date_str,
                                        'action': '卖出',
                                        'price': buy_price,
                                        'shares': missed_buy_fund_position,
                                        'profit': sell_profit
                                    })
                                    # 重置持仓，现金保留
                                    missed_buy_fund_position = 0
                                    missed_buy_fund_buy_price = 0
                                    missed_fund_total_bought_shares = 0
                                    missed_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                                    missed_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                                
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
                                    main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                                    main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                                    main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                                    # 重新计算可买入股数
                                    new_position = int(cash / buy_price)
                                
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                actual_condition_a_buy_count += 1  # 实际执行A买入的次数
                                is_condition_c_trade = False
                                is_condition_a_trade = True
                                action = f"买入A@{buy_price:.2f}"
                                trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '买入',
                                    'price': buy_price,
                                    'shares': position,
                                    'is_condition_c': False
                                })
                                # 记录锚定价格（用于可买未买资金账户）
                                if ENABLE_MISSED_BUY_FUND:
                                    last_anchor_price = buy_price
                                    main_account_has_position = True
                                    is_between_sell_and_buy = False  # 标记为买入与卖出之间
                                    # 重置买入档位触发记录（新的一次A买入周期开始）
                                    missed_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                                    missed_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                                    missed_fund_total_bought_shares = 0  # 重置总买入股数
                                volatility_declining_days = 0
                                # 初始化持仓期间价格追踪变量
                                hold_days = 0
                                highest_price_since_buy = 0
                                lowest_price_since_buy = 0
                                price_trend_direction = None
                                # 同步更新虚拟仓（完全镜像）
                                virtual_position = position
                                init_a_cycle(buy_price)
        
        # 新策略：可买未买资金在买入A与卖出区间内分批买入卖出
        if ENABLE_MISSED_BUY_FUND and last_anchor_price > 0:
            # 根据状态决定锚定价格
            # 卖出与买入之间（is_between_sell_and_buy=True）：使用卖出价格作为锚定，主账户可以无持仓
            # 买入与卖出之间（is_between_sell_and_buy=False）：使用买入A价格作为锚定，需要主账户有持仓
            if is_between_sell_and_buy:
                anchor_price = last_anchor_price
                allow_buy = False  # 卖出与买入之间，可买未买资金不买入（由主账户操作）
            else:
                anchor_price = last_anchor_price
                allow_buy = position > 0 and main_account_has_position  # 买入与卖出之间需要主账户有持仓
            
            if not allow_buy:
                anchor_price = 0  # 不允许买入时重置锚定价格
            
            # 计算当前价格相对于锚定价格的跌幅
            price_drop_pct = (close_price - anchor_price) / anchor_price if anchor_price > 0 else 0
            
            # 分批买入逻辑 - 所有满足条件的档位都触发
            triggered_buy_levels = []
            for i, (level, ratio) in enumerate(zip(MISSED_BUY_LEVELS, MISSED_BUY_RATIOS)):
                if not missed_buy_levels_triggered[i] and price_drop_pct <= level:
                    triggered_buy_levels.append(i)
            
            # 如果有满足条件的档位，执行买入
            for i in triggered_buy_levels:
                ratio = MISSED_BUY_RATIOS[i]
                buy_amount = initial_capital * ratio  # 使用固定初始资金的比例
                new_position = int(buy_amount / close_price)
                if new_position > 0 and missed_buy_fund_cash >= new_position * close_price:
                    cost = new_position * close_price
                    missed_buy_fund_cash -= cost
                    # 更新加权平均买入价格
                    if missed_buy_fund_position == 0:
                        missed_buy_fund_buy_price = close_price
                    else:
                        missed_buy_fund_buy_price = (missed_buy_fund_buy_price * missed_buy_fund_position + close_price * new_position) / (missed_buy_fund_position + new_position)
                    missed_buy_fund_position += new_position
                    missed_fund_total_bought_shares += new_position  # 记录总买入股数
                    missed_buy_fund_trade_count += 1
                    missed_buy_fund_trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': close_price,
                        'shares': new_position,
                        'level': i + 1
                    })
                    missed_buy_levels_triggered[i] = True
            
            # 显示触发的所有档位
            if triggered_buy_levels:
                triggered_levels_str = ','.join([f"买{i+1}" for i in triggered_buy_levels])
                action = f"未买资金{triggered_levels_str}@{close_price:.2f} 持仓{missed_buy_fund_position}"
            
            # 分批卖出逻辑（基于ATR倍数）- 所有满足条件的档位都触发（全部卖出）
            if missed_buy_fund_position > 0 and missed_buy_fund_buy_price > 0:
                # 获取当前ATR值
                current_atr = row['atr14'] if pd.notna(row['atr14']) else 0
                if current_atr > 0:
                    # 计算当前价格相对于买入价格的涨幅（以ATR为单位）
                    price_atr_multiplier = (close_price - missed_buy_fund_buy_price) / current_atr
                    
                    # 检查是否满足任何卖出档位
                    max_triggered_level = -1
                    for i in range(len(MISSED_SELL_ATR_MULTIPLIERS) - 1, -1, -1):
                        if price_atr_multiplier >= MISSED_SELL_ATR_MULTIPLIERS[i]:
                            max_triggered_level = i
                            break
                    
                    # 如果满足任何档位，全部卖出
                    if max_triggered_level >= 0:
                        # 计算总卖出比例（所有满足条件的档位比例之和）
                        total_ratio = sum(MISSED_SELL_RATIOS[:max_triggered_level + 1])
                        # 基于当前持仓计算卖出股数
                        sell_shares = int(missed_buy_fund_position * total_ratio)
                        # 确保不超过当前持仓
                        sell_shares = min(sell_shares, missed_buy_fund_position)
                        if sell_shares > 0:
                            sell_value = sell_shares * close_price
                            profit = (close_price - missed_buy_fund_buy_price) * sell_shares
                            missed_buy_fund_cash += sell_value
                            missed_buy_fund_position -= sell_shares
                            missed_buy_fund_trade_count += 1
                            missed_buy_fund_trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '卖出',
                                'price': close_price,
                                'shares': sell_shares,
                                'profit': profit,
                                'level': max_triggered_level + 1
                            })
                            # 显示触发的所有档位和剩余持仓
                            triggered_levels = ','.join([f"卖{i+1}" for i in range(max_triggered_level + 1)])
                            remaining_position = missed_buy_fund_position  # 卖出后的剩余持仓
                            action = f"未卖资金{triggered_levels}@{close_price:.2f} 持仓{remaining_position}"
                            # 如果持仓为0，重置买入价格
                            if missed_buy_fund_position == 0:
                                missed_buy_fund_buy_price = 0
        
        # 主账户在卖出A与买入A之间的分批买入卖出逻辑
        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and position == 0 and (main_account_drop_anchor_price > 0 or main_account_rise_anchor_price > 0):
            drop_anchor_price = main_account_drop_anchor_price if main_account_drop_anchor_price > 0 else last_anchor_price
            rise_anchor_price = main_account_rise_anchor_price if main_account_rise_anchor_price > 0 else last_anchor_price
            price_drop_pct = (close_price - drop_anchor_price) / drop_anchor_price if drop_anchor_price > 0 else 0

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
                        buy_amount = initial_capital * rise_ratio
                        new_position = int(buy_amount / close_price)
                        if new_position > 0 and cash >= new_position * close_price:
                            cost = new_position * close_price
                            cash -= cost
                            if main_account_sell_buy_position == 0:
                                main_account_sell_buy_price = close_price
                            else:
                                main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
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
                            executed_rise_levels.append(rise_idx)

                    if executed_rise_levels:
                        rise_levels_str = ','.join([f"\u8ffd\u6da8\u4e70{idx+1}" for idx in executed_rise_levels])
                        action = f"\u4e3b\u8d26\u6237{rise_levels_str}@{close_price:.2f} \u6301\u4ed3{main_account_sell_buy_position}"
            if close_price < ma20 and drop_anchor_price > 0:
                executed_drop_levels = []
                for drop_idx, level in enumerate(MISSED_BUY_LEVELS):
                    if main_account_sell_buy_levels_triggered[drop_idx]:
                        continue
                    if price_drop_pct > level:
                        continue

                    ratio = MISSED_BUY_RATIOS[drop_idx]
                    buy_amount = initial_capital * ratio
                    new_position = int(buy_amount / close_price)
                    if new_position <= 0 or cash < new_position * close_price:
                        continue

                    cost = new_position * close_price
                    cash -= cost
                    if main_account_sell_buy_position == 0:
                        main_account_sell_buy_price = close_price
                    else:
                        main_account_sell_buy_price = (
                            main_account_sell_buy_price * main_account_sell_buy_position
                            + close_price * new_position
                        ) / (main_account_sell_buy_position + new_position)

                    main_account_sell_buy_position += new_position
                    main_account_sell_buy_total_shares += new_position
                    trade_count += 1
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '\u4e70\u5165',
                        'price': close_price,
                        'shares': new_position,
                        'level': drop_idx + 1
                    })
                    main_account_sell_buy_levels_triggered[drop_idx] = True
                    executed_drop_levels.append(drop_idx)

                if executed_drop_levels:
                    drop_levels_str = ','.join([f"\u4e70{idx + 1}" for idx in executed_drop_levels])
                    action = f"\u4e3b\u8d26\u6237{drop_levels_str}@{close_price:.2f} \u6301\u4ed3{main_account_sell_buy_position}"

            if main_account_sell_buy_position > 0 and main_account_sell_buy_price > 0:
                current_atr = row['atr14'] if pd.notna(row['atr14']) else 0
                if current_atr > 0:
                    price_atr_multiplier = (close_price - main_account_sell_buy_price) / current_atr

                    if main_account_had_rise_entry_in_cycle:
                        sell_atr_levels = MAIN_ACCOUNT_UPTREND_SELL_ATR_MULTIPLIERS
                        sell_ratios = MAIN_ACCOUNT_UPTREND_SELL_RATIOS
                    else:
                        sell_atr_levels = MISSED_SELL_ATR_MULTIPLIERS
                        sell_ratios = MISSED_SELL_RATIOS

                    max_triggered_level = -1
                    for sell_idx in range(len(sell_atr_levels) - 1, -1, -1):
                        if price_atr_multiplier >= sell_atr_levels[sell_idx]:
                            max_triggered_level = sell_idx
                            break

                    if max_triggered_level >= 0:
                        total_ratio = sum(sell_ratios[:max_triggered_level + 1])
                        sell_shares = int(main_account_sell_buy_position * total_ratio)
                        sell_shares = min(sell_shares, main_account_sell_buy_position)
                        remaining_after_sell = main_account_sell_buy_position - sell_shares
                        if remaining_after_sell <= MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR:
                            sell_shares = main_account_sell_buy_position
                        if sell_shares > 0:
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
                                'level': max_triggered_level + 1
                            })
                            triggered_levels = ','.join([f"卖{i+1}" for i in range(max_triggered_level + 1)])
                            remaining_position = main_account_sell_buy_position
                            action = f"\u4e3b\u8d26\u6237{triggered_levels}@{close_price:.2f} \u6301\u4ed3{remaining_position}"
                            if main_account_sell_buy_position == 0:
                                main_account_sell_buy_price = 0
                                main_account_sell_buy_total_shares = 0
                                main_account_sell_buy_levels_triggered = [False] * len(MISSED_BUY_LEVELS)
                                main_account_sell_sell_levels_triggered = [False] * len(MISSED_SELL_ATR_MULTIPLIERS)
                                main_account_rise_buy_levels_triggered = [False] * len(MAIN_ACCOUNT_UPTREND_LEVELS)
                                if main_account_had_rise_entry_in_cycle:
                                    main_account_rise_reentry_locked = True
                                main_account_had_rise_entry_in_cycle = False

        display_position = position
        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
            display_position += main_account_sell_buy_position
        market_value = cash + display_position * close_price if display_position > 0 else cash
        position_str = f"{display_position}" if display_position > 0 else "0"
        
        # 数据显示（所有情况都显示，包括可买未买）
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        ma20_pct_str = f"{ma20_pct:.2f}" if pd.notna(ma20_pct) else "N/A"
        atr14_str = f"{row['atr14']:.2f}" if pd.notna(row['atr14']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        volatility_pct_str = f"{row['波动率百分比']:.1f}" if pd.notna(row['波动率百分比']) else "N/A"
        volatility_change_pct_str = f"{row['波幅变化%']:.1f}" if pd.notna(row['波幅变化%']) else "N/A"
        price_atr_ratio_str = f"{row['价ATR倍']:.2f}" if pd.notna(row['价ATR倍']) else "N/A"
        high_5day_str = f"{row['5日最高']:.2f}" if pd.notna(row['5日最高']) else "N/A"
        low_5day_str = f"{row['5日最低']:.2f}" if pd.notna(row['5日最低']) else "N/A"
        high_20day_str = f"{row['20日最高']:.2f}" if pd.notna(row['20日最高']) else "N/A"

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {ma20_pct_str:>8} {atr14_str:>8} {volatility_str:>8} {volatility_pct_str:>8} {volatility_change_pct_str:>10} {price_atr_ratio_str:>8} {high_5day_str:>8} {low_5day_str:>8} {high_20day_str:>8} {volatility_above_one_days:>6} {action:<12} {position_str:>8} {market_value:>12,.2f}")
    
    # 回测结束，如果有未处理的可买未买缓冲（回测在可买未买期间结束），不买入
    # 因为可买未买资金需要在连续可买未买结束后的第一天买入
    if consecutive_missed_days > 0:
        pass  # 回测在可买未买期间结束，不执行买入
    
    # 计算最终收益（主仓 + 主账户区间仓位）
    final_position = position
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        final_position += main_account_sell_buy_position
    final_value = cash + final_position * df.iloc[-1]['收盘'] if final_position > 0 else cash
    final_profit = final_value - initial_capital
    
    # 统计条件C交易数据
    condition_c_count = 0
    condition_c_loss_count = 0
    condition_c_total_profit = 0
    
    if trades:
        for idx, trade in enumerate(trades):
            if trade.get('is_condition_c') and trade['action'] == '买入':
                condition_c_count += 1
                # 找到对应的卖出交易
                for sell_trade in trades[idx+1:]:
                    if sell_trade['action'] == '卖出':
                        profit = sell_trade.get('profit', 0)
                        condition_c_total_profit += profit
                        if profit < 0:
                            condition_c_loss_count += 1
                        break
    
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
    log_print(f"  可买未买组数(连续算一组): {missed_buy_groups_count}")
    if total_condition_a_count > 0:
        log_print(f"  因持仓错过天数: {missed_buy_groups_count} ({missed_buy_groups_count/total_condition_a_count*100:.1f}%)")
    log_print(f"\n条件C交易统计:")
    log_print(f"  买入次数: {condition_c_count}")
    log_print(f"  亏损次数: {condition_c_loss_count}")
    log_print(f"  总盈利: {condition_c_total_profit:,.2f}")
    
    # 可买未买资金账户统计
    if ENABLE_MISSED_BUY_FUND:
        # 当前轮次的最终价值 = 现金 + 持仓 × 当前股价
        missed_buy_fund_final_value = missed_buy_fund_cash + missed_buy_fund_position * df.iloc[-1]['收盘'] if missed_buy_fund_position > 0 else missed_buy_fund_cash
        # 累计总盈利 = 最终资金 - 初始资金
        missed_buy_fund_total_profit = missed_buy_fund_final_value - initial_capital
        log_print(f"\n【可买未买资金账户统计】(独立运营)")
        log_print(f"  起始资金: {initial_capital:,.2f}")
        log_print(f"  最终资金: {missed_buy_fund_final_value:,.2f}")
        log_print(f"  总盈利: {missed_buy_fund_total_profit:,.2f}")
        log_print(f"  收益率: {(missed_buy_fund_total_profit/initial_capital)*100:.2f}%")
        log_print(f"  交易次数: {missed_buy_fund_trade_count}")
        if missed_buy_fund_position > 0:
            log_print(f"  当前持仓: {missed_buy_fund_position}股 (买入均价: {missed_buy_fund_buy_price:.2f})")
        
        if missed_buy_fund_trades:
            log_print(f"\n  可买未买资金账户交易明细:")
            log_print(f"  {'序号':<6} {'日期':<12} {'操作':<6} {'价格':>10} {'股数':>10} {'盈亏':>12}")
            log_print(f"  {'-' * 70}")
            for idx, trade in enumerate(missed_buy_fund_trades, 1):
                profit_str = f"{trade.get('profit', 0):,.2f}" if 'profit' in trade else "-"
                level_str = f"(档位{trade.get('level', '-')})" if trade.get('level') else ""
                log_print(f"  {idx:<6} {trade['date']:<12} {trade['action']:<6} {trade['price']:>10.2f} {trade['shares']:>10} {profit_str:>12} {level_str}")

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
        
        # 1. 延迟卖出状态 - 最严格的触发条件
        if is_pending_sell and ENABLE_DELAYED_SELL:
            sell_triggers.append({
                'name': '延迟卖出',
                'price': pending_sell_price,
                'condition': f'价格 ≤ {pending_sell_price:.2f}',
                'priority': 1  # 最高优先级
            })
        
        # 2. 价格追踪状态
        if ENABLE_STOP_LOSS and hold_days >= 2 and highest_price_since_buy > 0 and lowest_price_since_buy > 0:
            if price_trend_direction == 'up':
                # 上涨趋势：跌破最高价触发卖出
                sell_triggers.append({
                    'name': '趋势上涨破高',
                    'price': highest_price_since_buy,
                    'condition': f'价格 < {highest_price_since_buy:.2f}',
                    'priority': 2
                })
            elif price_trend_direction == 'down':
                # 下跌趋势：理论上应该已经卖出，但以防万一
                sell_triggers.append({
                    'name': '趋势下跌',
                    'price': last_close * 0.99,  # 近似
                    'condition': '价格继续下跌',
                    'priority': 2
                })
            else:
                # 趋势未确定：等待确定方向
                # 如果下一天价格下跌，会进入下跌趋势并触发卖出
                sell_triggers.append({
                    'name': '趋势确定-下跌',
                    'price': last_close * 0.99,
                    'condition': f'价格 < {last_close:.2f}',
                    'priority': 3
                })
        
        # 3. MA20阈值止损 - 仅在非待卖出状态时计算
        if ENABLE_STOP_LOSS and pd.notna(last_ma20) and not is_pending_sell:
            target_price_stop = last_ma20 / (1 + STOP_LOSS_MA20_THRESHOLD / 100)
            sell_triggers.append({
                'name': 'MA20阈值止损',
                'price': target_price_stop,
                'condition': f'价格 ≤ {target_price_stop:.2f}',
                'priority': 4
            })
        
        # 4. 波动率比率卖出（仅普通状态，非价格追踪状态，非待卖出状态）
        if not (ENABLE_STOP_LOSS and hold_days >= 2 and highest_price_since_buy > 0) and not is_pending_sell:
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
        
        # 5. 条件C卖出（仅在非待卖出状态时计算）
        if is_condition_c_trade and not is_pending_sell:
            if pd.notna(last_atr14) and last_atr14 > 0 and len(df) >= 5:
                ma20_t_minus_4 = df.iloc[-5]['ma20'] if pd.notna(df.iloc[-5]['ma20']) else last_ma20
                target_volatility_c = BUY_CONDITION_C_VOL_THRESHOLD
                target_ma20_change_c = target_volatility_c * last_atr14
                target_price_c = (target_ma20_change_c + ma20_t_minus_4) * 20 - last_ma20 * 19
                
                sell_triggers.append({
                    'name': '条件C卖出',
                    'price': target_price_c,
                    'condition': f'波动率降至{BUY_CONDITION_C_VOL_THRESHOLD}以下',
                    'priority': 6
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
    
    return total_return, yearly_returns, {'trades': trades, 'final_value': final_value}


if __name__ == "__main__":
    run_backtest()






































