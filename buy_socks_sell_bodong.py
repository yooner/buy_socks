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

# 卖出A优化配置：当收盘价高于MA20一定比例时，延迟卖出直到价格回到MA20以下
ENABLE_SELL_A_DELAYED = True  # 是否启用卖出A延迟卖出功能
SELL_A_MA20_THRESHOLD_PCT = 0.11  # 收盘价高于MA20该比例时（默认11%），延迟卖出直到价格回到MA20以下
SELL_A_MA20_SELL_RATIO = 0.97  # 延迟卖出时，价格需低于MA20的该比例才卖出（默认0.97即低于MA20的3%）
# N日高价卖出条件（使用MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS作为周期）
SELL_A_HIGH_DROP_RATIO = 0.90  # 收盘价低于N日高价的该比例时卖出（默认0.95即下跌5%）
# 持仓期间最高价卖出条件：当收盘价低于持仓期间最高价的设定百分比时卖出
SELL_A_HOLDING_HIGH_DROP_RATIO = 0.50  # 收盘价低于持仓期间最高价的该比例时卖出（默认0.07即下跌7%）

# 普通买入A与卖出A的止盈机制配置
ENABLE_NORMAL_TAKE_PROFIT = False  # 是否启用普通买入A与卖出A的止盈机制
NORMAL_TAKE_PROFIT_LEVELS = [0.10, 0.20, 0.30, 0.40]  # 止盈档位（10%, 20%, 30%, 40%）
NORMAL_TAKE_PROFIT_RATIOS = [0.10, 0.20, 0.30, 0.40]  # 各档位卖出比例（10%, 20%, 30%, 40%）
NORMAL_TAKE_PROFIT_RISING_DAYS = 2  # 价格趋势连续上升天数阈值，达到该天数则延迟卖出

# 普通买入A与卖出A的连续N天未创新高卖出配置（独立参数）
ENABLE_NORMAL_NO_NEW_HIGH_SELL = True  # 是否启用普通买入A与卖出A的连续N天未创新高卖出机制
NORMAL_NO_NEW_HIGH_DAYS = 5  # 连续多少天未创新高就卖出（默认6天）
NORMAL_NO_NEW_HIGH_RATIO = 0.99  # 未创新高阈值，收盘价低于持仓最高价的该比例时视为未创新高（默认0.99即低于最高价1%）

# 普通卖出后的买回配置（防止卖了然后涨了）
NORMAL_SELL_BUYBACK_THRESHOLD = 0.03  # 买回阈值，价格高于卖出价格此比例时才买回（默认0.03即3%）

# 买入条件全局参数
BUY_DECLINE_DAYS_REQUIRED = 3  # 波动率连续向0靠近所需天数（条件A）

# 主账户在卖出A与买入A之间的分批买入卖出开关
ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING = True  # 设置为True启用：主账户在卖出A与买入A之间分批买入卖出

# 主账户区间交易分批买入配置
ENABLE_MAIN_ACCOUNT_BUY_BY_ATR = True  # 是否启用基于价ATR倍数的买入（False则使用基于跌幅的买入）
# 基于跌幅的买入配置
MAIN_ACCOUNT_BUY_LEVELS = [-0.13, -0.20]  # 买入触发跌幅（-4%, -8%, -13%）

# 追买机制配置（基于跌幅买入的追加买入）
ENABLE_MAIN_ACCOUNT_CHASE_BUY = True  # 是否启用追买机制
MAIN_ACCOUNT_CHASE_BUY_PRICE_DROP = 0.02  # 价格相对于上一次跌幅买入价格降低多少触发追买（默认0.02即2%）
MAIN_ACCOUNT_CHASE_BUY_RATIO = 0.20  # 追买比例（默认20%）
MAIN_ACCOUNT_CHASE_BUY_MAX_COUNT = 5  # 最大追买次数（防止无限追买）
# 基于价ATR倍数的买入配置
MAIN_ACCOUNT_BUY_ATR_LEVELS = [-2.8, -4.0, -5.0]  # 买入触发价ATR倍数阈值（对应买1: >-3, 买2: >-4, 买3: <=-4）
MAIN_ACCOUNT_BUY_ATR_CONSECUTIVE_DAYS = [2, 1, 1]  # 各档位需要的连续天数（买1需连续3天满足条件，买2买3只需1天）
# 买入比例配置
# 基于价ATR倍数的买入比例配置
MAIN_ACCOUNT_BUY_ATR_RATIOS = [0.80, 0.80, 1.0]      # 对应买入比例（20%, 30%, 50%）
# 基于跌幅的买入比例配置
MAIN_ACCOUNT_BUY_DROP_RATIOS = [0.30, 0.60]      # 对应买入比例（20%, 30%, 50%）

# 主账户区间交易分批卖出配置
# 基于价ATR倍数的卖出配置（对应基于价ATR倍数的买入）
MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS = [0, 1.0, 2.0, 3.0]  # 卖出触发ATR倍数（0, 1.0, 2.0, 3.0）
# 基于补跌买入比例的卖出配置（对应基于跌幅的买入）
MAIN_ACCOUNT_SELL_PRICE_DROP_MULTIPLIERS = [0, 1.0, 2.0, 3.0]  # 卖出触发ATR倍数（0, 1.0, 2.0, 3.0）
# 卖出比例配置（两种方式共用）
MAIN_ACCOUNT_SELL_RATIOS = [0.30, 0.30, 0.25, 0.15]          # 对应卖出比例（30%, 30%, 40%）

# ATR周期配置
ATR_PERIOD = 14  # ATR计算周期，可配置为10、14、20等
ATR_PERCENTILE_DAYS = 60  # ATR分位数计算周期（默认60天）
ATR_PERCENTILE_TREND_THRESHOLD = 0.005 # ATR分位数趋势判断阈值（5%），超过此变化才认为是趋势变化

# 价格分位配置
PRICE_PERCENTILE_DAYS = 120  # 价格分位计算周期（默认120天，可配置为60、90、120等）
PRICE_PERCENTILE_MIN_PERIODS = 60  # 价格分位计算最小周期数（默认60天）

# 主账户区间交易：剩余仓位小于等于该阈值时直接清仓，避免长期残仓
MAIN_ACCOUNT_MIN_REMAIN_SHARES_TO_CLEAR = 300

# 爆发买入机制（卖出A与买入A之间）
ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY = True  # 是否启用爆发买入机制
MAIN_ACCOUNT_OUTBREAK_BUY_CONSECUTIVE_DAYS = 2  # 价ATR倍连续大于阈值的天数
MAIN_ACCOUNT_OUTBREAK_BUY_PRICE_ATR_THRESHOLD = 0.8  # 价ATR倍买入阈值
# 爆发买入分仓配置
ENABLE_MAIN_ACCOUNT_OUTBREAK_POSITION_BUY = True  # 是否启用分仓买入（ATR趋势非上涨且分位>阈值时）
MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_PERCENTILE_THRESHOLD = 0.40  # 分位阈值（默认50%）
MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT = 4  # 分仓数量（默认4仓）
# 爆发卖出配置
MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS = 4  # 计算最高价的周期（默认5日，可设置为3日等）
MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD = 0.07  # 最高价下降超过该比例才卖出（防止小幅波动）
MAIN_ACCOUNT_OUTBREAK_SELL_DROP_THRESHOLD = -0.10  # 收盘价与N日最高价差距阈值，小于该值卖出（默认-10%）
MAIN_ACCOUNT_OUTBREAK_SELL_PREV_DAY_RATIO = 0.91  # 单日跌幅阈值，收盘价低于前一天该比例则卖出（默认0.96即跌幅4%）
MAIN_ACCOUNT_OUTBREAK_SELL_HOLDING_HIGH_THRESHOLD = 0.12  # 收盘价低于持仓期间最高价的阈值，超过该比例卖出（默认7%）
MAIN_ACCOUNT_OUTBREAK_SELL_ENABLE_MA20_CONDITION = False  # 是否启用跌破MA20的卖出条件（条件E）
# 连续N天未创新高卖出配置（替代跌破MA20卖出条件E）
MAIN_ACCOUNT_OUTBREAK_SELL_ENABLE_NO_NEW_HIGH_CONDITION = True  # 是否启用连续N天未创新高卖出条件（新条件E）
MAIN_ACCOUNT_OUTBREAK_SELL_NO_NEW_HIGH_DAYS = 6  # 连续多少天未创新高就卖出（默认5天）
MAIN_ACCOUNT_OUTBREAK_SELL_NO_NEW_HIGH_RATIO = 0.99  # 未创新高阈值，收盘价低于持仓最高价的该比例时视为未创新高（默认0.98即低于最高价2%）

# 卖出E后的买回配置
MAIN_ACCOUNT_OUTBREAK_SELL_E_BUYBACK_THRESHOLD = 0.03  # 买回阈值，价格高于卖出E价格此比例时才买回（默认0.03即3%）

# 爆发模式止盈卖出配置
ENABLE_MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT = False  # 是否启用爆发模式止盈卖出
MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT_RATIO = 0.10  # 每次止盈卖出比例（默认10%）
MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT_PRICE_DROP_THRESHOLD = 0.03  # 价格下跌阈值（默认0.025即2.5%），低于前一天价格此比例则卖出

# 买入A延迟买入开关
ENABLE_BUY_A_DELAYED = True  # 是否启用延迟买入
# 新延迟买入规则参数
BUY_A_DELAYED_NEW_ENABLE = True  # 是否启用新的延迟买入规则
BUY_A_DELAYED_NEW_HIT_DAYS = 1  # 价ATR倍 < 5日价ATR倍累计天数
BUY_A_DELAYED_NEW_FIVE_DAY_ATR_THRESHOLD = -1.1  # 5日价ATR倍阈值
BUY_A_DELAYED_NEW_FORCE_BUY_ATR = -3.0  # 强制满仓的价ATR倍阈值

CLOSE_PRICE_TREND_THRESHOLD = 0.005  # 收盘价格趋势判断阈值（1%）


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
    
    # 计算ATR（使用可配置周期）
    prev_close = df['收盘'].shift(1)
    tr1 = df['最高'] - df['最低']
    tr2 = (df['最高'] - prev_close).abs()
    tr3 = (df['最低'] - prev_close).abs()
    df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = df['tr'].rolling(window=ATR_PERIOD, min_periods=ATR_PERIOD).mean()
    
    # 计算波动率（使用可配置ATR周期）
    df = calculate_slope_atr(df, ma_period=20, atr_period=ATR_PERIOD, n=5)
    
    # 计算价格相对ATR的倍数：(收盘价 - MA20) / ATR
    df['价ATR倍'] = ((df['收盘'] - df['ma20']) / df['atr']).replace([np.inf, -np.inf], np.nan)
    # 计算ATR分位数：当前ATR在过去N天中的分位值
    # atr_pct[t] = count(ATR[i] <= ATR[t]) / N
    def calculate_atr_percentile(series, window=ATR_PERCENTILE_DAYS):
        """计算ATR在过去N天中的分位数"""
        result = pd.Series(index=series.index, dtype=float)
        for i in range(len(series)):
            if i < window - 1:
                # 数据不足N天，使用已有数据计算
                window_data = series.iloc[:i+1].dropna()
            else:
                window_data = series.iloc[i-window+1:i+1].dropna()
            if len(window_data) > 0 and pd.notna(series.iloc[i]):
                current_atr = series.iloc[i]
                # 计算分位数：小于等于当前值的个数 / 总数
                count_le = (window_data <= current_atr).sum()
                result.iloc[i] = count_le / len(window_data)
            else:
                result.iloc[i] = np.nan
        return result
    
    df['atr_pct'] = calculate_atr_percentile(df['atr'], window=ATR_PERCENTILE_DAYS)
    
    # 计算ATR分位数趋势（上升/下降/平稳）
    # 使用5日移动平均来平滑，然后判断趋势
    df['atr_pct_ma5'] = df['atr_pct'].rolling(window=7, min_periods=4).mean()
    df['atr_pct_trend'] = ''
    
    for i in range(len(df)):
        if i < 5 or pd.isna(df.loc[i, 'atr_pct_ma5']) or pd.isna(df.loc[i-1, 'atr_pct_ma5']):
            df.loc[i, 'atr_pct_trend'] = ''
            continue
        
        current_ma = df.loc[i, 'atr_pct_ma5']
        prev_ma = df.loc[i-1, 'atr_pct_ma5']
        change = current_ma - prev_ma
        
        if abs(change) < ATR_PERCENTILE_TREND_THRESHOLD:
            df.loc[i, 'atr_pct_trend'] = '→'  # 平稳
        elif change > 0:
            df.loc[i, 'atr_pct_trend'] = '↑'  # 上升
        else:
            df.loc[i, 'atr_pct_trend'] = '↓'  # 下降
    
    # 计算价格分位（收盘价在过去N天中的百分位，N可配置）
    df['price_pct'] = df['收盘'].rolling(window=PRICE_PERCENTILE_DAYS, min_periods=PRICE_PERCENTILE_MIN_PERIODS).apply(
        lambda x: (x.iloc[-1] - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0.5, raw=False
    )
    
    # 计算收盘价格趋势（使用5日移动平均，阈值1%）
    
    df['close_ma5'] = df['收盘'].rolling(window=3, min_periods=2).mean()
    df['close_trend'] = ''
    
    for i in range(len(df)):
        if i < 5 or pd.isna(df.loc[i, 'close_ma5']) or pd.isna(df.loc[i-1, 'close_ma5']):
            df.loc[i, 'close_trend'] = ''
            continue
        
        current_ma = df.loc[i, 'close_ma5']
        prev_ma = df.loc[i-1, 'close_ma5']
        change_pct = (current_ma - prev_ma) / prev_ma if prev_ma != 0 else 0
        
        if abs(change_pct) < CLOSE_PRICE_TREND_THRESHOLD:
            df.loc[i, 'close_trend'] = '→'  # 平稳
        elif change_pct > 0:
            df.loc[i, 'close_trend'] = '↑'  # 上升
        else:
            df.loc[i, 'close_trend'] = '↓'  # 下降
    
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
    
    # 计算每日收盘/MA20百分比
    df['收盘_MA20百分比'] = (df['收盘'] / df['ma20'] - 1) * 100
    # 计算10日平均收盘/MA20百分比
    df['10日平均收盘_MA20'] = df['收盘_MA20百分比'].rolling(window=10, min_periods=1).mean()
    
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
    
    # 卖出A延迟卖出状态变量
    sell_a_delayed_pending = False  # 是否有待卖A（当价格高于MA20阈值时延迟卖出）
    
    # 普通买入A与卖出A的止盈状态变量
    normal_take_profit_levels_triggered = [False] * len(NORMAL_TAKE_PROFIT_LEVELS)  # 各止盈档位是否已触发
    normal_take_profit_buy_price = 0  # 买入A的价格（用于计算止盈比例）
    normal_take_profit_rising_days = 0  # 价格连续上升天数
    normal_take_profit_cash_locked = 0  # 止盈卖出后锁定的资金（模拟资金为0的效果）
    
    # 普通买入A与卖出A的连续N天未创新高卖出状态变量
    normal_no_new_high_days = 0  # 连续未创新高天数
    normal_last_high_price = 0  # 持仓期间最高价
    
    # 普通卖出后的买回状态变量
    normal_sell_buyback_active = False  # 是否处于普通卖出后的买回模式
    normal_sell_buyback_price = 0  # 普通卖出价格（用于计算买回阈值）
    
    # 跌幅买入延迟状态变量
    drop_buy_delayed_pending = False  # 是否有待跌幅买入（趋势下降时标记，等待趋势变平或变高）
    drop_buy_delayed_level = -1  # 待跌幅买入的档位（0=买1, 1=买2, 2=买3）
    drop_buy_delayed_type = ''  # 待跌幅买入的类型（'ATR'或'DROP'）
    
    # 主账户在卖出A与买入A之间的分批买入卖出状态变量
    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
        # 两种买入模式的档位状态（并行工作）
        main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)  # 基于价ATR倍数的买入档位
        main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)  # 各档位连续满足条件天数计数
        main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)  # 基于跌幅的买入档位
        main_account_sell_buy_position = 0  # 主账户在卖出A与买入A之间的持仓
        main_account_sell_buy_price = 0  # 主账户在卖出A与买入A之间的加权平均买入价格
        # 卖出档位（只使用ATR卖出模式）
        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)  # 卖出档位
        main_account_drop_anchor_price = 0
        main_account_initial_cash = 0  # 主账户区间交易的初始资金（卖出时的现金）
        # 爆发买入机制状态变量
        main_account_outbreak_buy_consecutive_days = 0  # 波动率连续大于阈值天数
        main_account_outbreak_buy_active = False  # 是否处于爆发买入持仓状态
        main_account_outbreak_buy_price = 0  # 爆发买入价格
        main_account_outbreak_sell_high = 0  # 爆发买入后的N日最高价
        # 爆发模式止盈卖出状态变量
        main_account_outbreak_take_profit_active = False  # 是否处于止盈模式（分位100%且趋势非上升时激活）
        main_account_outbreak_take_profit_prev_close = 0  # 前一天收盘价（用于计算跌幅）
        # 持仓期间最高价（适用于所有持仓类型：主仓、区间交易、爆发买入等）
        main_account_holding_high = 0
        # 爆发买入分仓状态变量
        main_account_outbreak_position_buy_active = False  # 是否处于分仓买入模式
        main_account_outbreak_position_buy_count = 0  # 已买入仓位数量
        main_account_outbreak_position_buy_prices = []  # 各仓位买入价格记录
        main_account_outbreak_position_buy_pending = False  # 是否等待第二天下跌买入
        main_account_outbreak_position_buy_trigger_price = 0  # 触发分仓买入的价格（爆发点价格）
        # 追买机制状态变量
        main_account_chase_buy_last_price = 0  # 上一次跌幅买入的价格（用于追买计算）
        main_account_chase_buy_count = 0  # 已追买次数
        # 连续未创新高卖出状态变量
        main_account_no_new_high_days = 0  # 连续未创新高天数计数器
        main_account_last_high_price = 0  # 上一次创新高的价格
        # 爆发卖出E后的买回机制状态变量
        main_account_outbreak_sell_e_active = False  # 是否处于卖出E后的买回模式
        main_account_outbreak_sell_e_price = 0  # 卖出E的价格
    else:
        main_account_sell_buy_levels_triggered_atr = []
        main_account_sell_buy_levels_consecutive_days = []
        main_account_sell_buy_levels_triggered_drop = []
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
        # 爆发模式止盈卖出状态变量
        main_account_outbreak_take_profit_active = False  # 是否处于止盈模式（分位100%且趋势非上升时激活）
        main_account_outbreak_take_profit_prev_close = 0  # 前一天收盘价（用于计算跌幅）
        # 持仓期间最高价（适用于所有持仓类型：主仓、区间交易、爆发买入等）
        main_account_holding_high = 0
        # 爆发买入分仓状态变量
        main_account_outbreak_position_buy_active = False
        main_account_outbreak_position_buy_count = 0
        main_account_outbreak_position_buy_prices = []
        main_account_outbreak_position_buy_pending = False
        main_account_outbreak_position_buy_trigger_price = 0
        # 追买机制状态变量
        main_account_chase_buy_last_price = 0
        main_account_chase_buy_count = 0
        # 连续未创新高卖出状态变量
        main_account_no_new_high_days = 0
        main_account_last_high_price = 0
        # 爆发卖出E后的买回机制状态变量
        main_account_outbreak_sell_e_active = False
        main_account_outbreak_sell_e_price = 0

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

    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'MA20':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'价ATR倍':>8} {'atr_pct':>8} {'趋势':>6} {f'价分位{PRICE_PERCENTILE_DAYS}':>8} {'5日ATR平均':>8} {'10日最低价ATR倍数':>16} {f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高':>8} {'持仓最高':>8}   {'操作':<30} {'持仓':>8} {'市值':>12}"
    log_print(header)
    log_print("-" * 185)
    
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
                # 初始化连续未创新高计数
                if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                    main_account_last_high_price = close_price
                    main_account_no_new_high_days = 0
            elif close_price > main_account_holding_high:
                # 价格创新高，更新最高价
                main_account_holding_high = close_price
                # 重置连续未创新高计数
                if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                    main_account_last_high_price = close_price
                    main_account_no_new_high_days = 0
            else:
                # 检查是否低于最高价的阈值比例
                if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and main_account_last_high_price > 0:
                    price_ratio = close_price / main_account_last_high_price
                    if price_ratio < MAIN_ACCOUNT_OUTBREAK_SELL_NO_NEW_HIGH_RATIO:
                        # 低于阈值比例，增加未创新高计数
                        main_account_no_new_high_days += 1
                    else:
                        # 虽然未创新高，但在阈值范围内，重置计数
                        main_account_no_new_high_days = 0
        
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
            
            # 爆发C条件检查：单日跌幅超过阈值（适用于普通持仓）
            outbreak_c_triggered = False
            if position > 0 and i > 0:
                prev_close = df.iloc[i-1]['收盘']
                if close_price < prev_close * MAIN_ACCOUNT_OUTBREAK_SELL_PREV_DAY_RATIO:
                    outbreak_c_triggered = True
            
            # 卖出A优化：检查是否高于MA20阈值
            if sell_a_signal_triggered and position > 0:
                if ENABLE_SELL_A_DELAYED:
                    # 启用延迟卖出功能
                    # 计算收盘价与MA20的比例
                    ma20_ratio = (close_price - ma20) / ma20 if pd.notna(ma20) and ma20 > 0 else 0
                    if ma20_ratio > SELL_A_MA20_THRESHOLD_PCT and not sell_a_delayed_pending:
                        # 价格高于MA20阈值，标记为待卖A，延迟卖出
                        sell_a_delayed_pending = True
                        # 重置持仓最高价为当前价格，从待卖A开始重新计算
                        main_account_holding_high = close_price
                        action = f"待卖A@{close_price:.2f}(高于MA20 {ma20_ratio*100:.1f}%)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '待卖A',
                            'price': close_price,
                            'shares': 0
                        })
                    elif sell_a_delayed_pending:
                        # 待卖A状态，检查两个卖出条件
                        # 条件1：价格低于MA20的设定比例
                        condition1 = close_price <= ma20 * SELL_A_MA20_SELL_RATIO
                        # 条件2：收盘价低于N日高价的设定比例（使用爆发卖出的N日周期配置）
                        high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                        current_high = row[high_col] if pd.notna(row[high_col]) else 0
                        condition2 = (current_high > 0 and 
                                     close_price <= current_high * SELL_A_HIGH_DROP_RATIO)
                        # 条件3：波动率满足卖出A条件（波动率>0且降低，且降至前一天阈值以下）
                        condition3 = False
                        if volatility > 0 and is_volatility_declining:
                            volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                            if volatility_ratio <= SELL_RATIO_THRESHOLD:
                                condition3 = True
                        if condition1 or condition2 or condition3:
                            should_sell = True
                            if condition1 and condition2:
                                sell_reason = "比率卖出(待卖A-MA20且N日高价触发)"
                            elif condition1:
                                sell_reason = "比率卖出(待卖A-MA20触发)"
                            elif condition2:
                                sell_reason = "比率卖出(待卖A-N日高价触发)"
                            else:
                                sell_reason = "比率卖出(待卖A-波动率触发)"
                            sell_a_delayed_pending = False
                        # 待卖A状态下，如果触发爆发C条件（单日大幅下跌），也应该卖出
                        elif outbreak_c_triggered:
                            should_sell = True
                            sell_reason = "爆发C卖出(待卖A状态)"
                            sell_a_delayed_pending = False
                    elif not sell_a_delayed_pending:
                        # 未触发延迟卖出条件，正常卖出
                        should_sell = True
                        sell_reason = "比率卖出"
                else:
                    # 未启用延迟卖出功能，正常卖出
                    should_sell = True
                    sell_reason = "比率卖出"
            
            # 爆发C条件触发卖出（普通持仓）
            if outbreak_c_triggered and position > 0 and not should_sell:
                should_sell = True
                sell_reason = "爆发C卖出"
            
            # 持仓期间最高价卖出条件：当收盘价低于持仓期间最高价的设定百分比时卖出
            if position > 0 and main_account_holding_high > 0 and not should_sell:
                holding_high_drop_pct = (main_account_holding_high - close_price) / main_account_holding_high
                if holding_high_drop_pct > SELL_A_HOLDING_HIGH_DROP_RATIO:
                    should_sell = True
                    sell_reason = f"比率卖出(持仓最高价下跌{holding_high_drop_pct*100:.1f}%)"
            
            # 普通买入A与卖出A的连续N天未创新高卖出逻辑
            if ENABLE_NORMAL_NO_NEW_HIGH_SELL and position > 0 and not should_sell:
                # 更新持仓期间最高价
                if close_price > normal_last_high_price:
                    normal_last_high_price = close_price
                    normal_no_new_high_days = 0
                else:
                    # 检查是否未创新高（收盘价低于持仓最高价的设定比例）
                    if normal_last_high_price > 0:
                        no_new_high_threshold = normal_last_high_price * NORMAL_NO_NEW_HIGH_RATIO
                        if close_price < no_new_high_threshold:
                            normal_no_new_high_days += 1
                        else:
                            normal_no_new_high_days = 0
                
                # 检查是否达到连续N天未创新高
                if normal_no_new_high_days >= NORMAL_NO_NEW_HIGH_DAYS:
                    should_sell = True
                    sell_reason = f"比率卖出(连续{normal_no_new_high_days}天未创新高)"
            
            # 普通买入A与卖出A的止盈逻辑
            if ENABLE_NORMAL_TAKE_PROFIT and position > 0 and normal_take_profit_buy_price > 0:
                # 计算当前盈利比例
                current_profit_ratio = (close_price - normal_take_profit_buy_price) / normal_take_profit_buy_price
                # 检查价格趋势（是否连续上升）
                if i > 0:
                    prev_close = df.iloc[i-1]['收盘']
                    if close_price > prev_close:
                        normal_take_profit_rising_days += 1
                    else:
                        normal_take_profit_rising_days = 0
                
                # 检查各止盈档位
                for level_idx in range(len(NORMAL_TAKE_PROFIT_LEVELS)):
                    if not normal_take_profit_levels_triggered[level_idx]:
                        if current_profit_ratio >= NORMAL_TAKE_PROFIT_LEVELS[level_idx]:
                            # 达到止盈档位
                            # 如果价格趋势连续上升达到阈值，则延迟卖出
                            if normal_take_profit_rising_days >= NORMAL_TAKE_PROFIT_RISING_DAYS:
                                # 价格连续上升，延迟卖出，继续持有
                                pass
                            else:
                                # 执行止盈卖出
                                sell_ratio = NORMAL_TAKE_PROFIT_RATIOS[level_idx]
                                sell_shares = int(position * sell_ratio / 100) * 100
                                if sell_shares >= 100:
                                    sell_price = close_price
                                    sell_value = sell_shares * sell_price
                                    profit = (sell_price - normal_take_profit_buy_price) * sell_shares
                                    cash += sell_value
                                    position -= sell_shares
                                    normal_take_profit_levels_triggered[level_idx] = True
                                    action = f"止盈卖出{level_idx+1}@{sell_price:.2f}({sell_ratio*100:.0f}%)"
                                    trades.append({
                                        'day': day_num,
                                        'date': date_str,
                                        'action': '卖出',
                                        'price': sell_price,
                                        'shares': sell_shares,
                                        'profit': profit,
                                        'type': f'止盈{level_idx+1}'
                                    })
                                    # 如果全部卖出，重置状态并锁定资金
                                    if position <= 0:
                                        position = 0
                                        buy_price = 0
                                        normal_take_profit_buy_price = 0
                                        volatility_declining_days = 0
                                        holding_start_date = None
                                        buy_a_delayed_pending = False
                                        buy_a_marked = False
                                        buy_a_pending_price = 0.0
                                        buy_a_below_ma20_atr_hit_count = 0
                                        sell_a_delayed_pending = False
                                        # 止盈全部卖出后，锁定资金（模拟资金为0，防止爆发买入触发）
                                        normal_take_profit_cash_locked = cash
                                        cash = 0
                                    break  # 每次只处理一个档位
            
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
                    # 重置卖出A延迟卖出状态
                    sell_a_delayed_pending = False
                    # 重置普通买入A与卖出A的连续N天未创新高卖出状态
                    if ENABLE_NORMAL_NO_NEW_HIGH_SELL:
                        normal_no_new_high_days = 0
                        normal_last_high_price = 0
                    
                    # 激活普通卖出后的买回模式（防止卖了然后涨了）
                    # 只有因连续N天未创新高卖出的情况才启用买回机制
                    # 注意：如果处于爆发买入状态，不启用普通买回机制（避免冲突）
                    if ENABLE_NORMAL_NO_NEW_HIGH_SELL and "连续" in sell_reason and "未创新高" in sell_reason and not main_account_outbreak_buy_active:
                        normal_sell_buyback_active = True
                        normal_sell_buyback_price = sell_price

                    # 主账户在卖出A与买入A之间的分批买入卖出状态变量重置
                    # 只有卖出A（波动率卖出）才设置锚定价格，爆发C卖出不设置
                    if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and sell_reason != "爆发C卖出":
                        # 设置锚定价格为卖出价格
                        main_account_drop_anchor_price = sell_price
                        # 记录卖出时的现金作为区间交易的初始资金
                        main_account_initial_cash = cash
                        # 重置主账户在卖出A与买入A之间的分批买入卖出状态变量
                        # 重置两种买入模式的档位
                        main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        # 注意：不重置main_account_sell_buy_position，因为区间交易持仓应该在买入A时才卖出
                        # main_account_sell_buy_position = 0
                        main_account_sell_buy_price = 0
                        # 重置卖出档位
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        # 重置追买状态
                        main_account_chase_buy_last_price = 0
                        main_account_chase_buy_count = 0
                        # 普通卖出时解锁止盈锁定的资金
                        if normal_take_profit_cash_locked > 0:
                            cash += normal_take_profit_cash_locked
                            normal_take_profit_cash_locked = 0
                    
                    # 卖出时同步重置爆发买入状态（解锁）
                    # 注意：只有在非卖出E锁定模式下才重置爆发买入状态
                    # 卖出E锁定模式下，需要等待真正的卖出A（波动率卖出）来解锁
                    if main_account_outbreak_buy_active and not main_account_outbreak_sell_e_active:
                        main_account_outbreak_buy_active = False
                        main_account_outbreak_buy_price = 0
                        main_account_outbreak_buy_consecutive_days = 0
                        main_account_outbreak_sell_high = 0
                        # 重置爆发模式止盈卖出状态
                        main_account_outbreak_take_profit_active = False
                        main_account_outbreak_take_profit_prev_close = 0
                        # 重置分仓买入状态
                        main_account_outbreak_position_buy_active = False
                        main_account_outbreak_position_buy_count = 0
                        main_account_outbreak_position_buy_prices = []
                        main_account_outbreak_position_buy_pending = False
                        main_account_outbreak_position_buy_trigger_price = 0
                    # 在卖出E锁定模式下，只有真正的卖出A（波动率卖出）才能解锁
                    # 检查是否是纯粹的波动率卖出（比率卖出，不带其他后缀）且处于卖出E锁定模式
                    elif main_account_outbreak_sell_e_active and sell_reason == "比率卖出":
                        # 真正的卖出A到达，解锁卖出E锁定模式
                        main_account_outbreak_sell_e_active = False
                        main_account_outbreak_sell_e_price = 0
                        # 同时重置爆发买入状态
                        main_account_outbreak_buy_active = False
                        main_account_outbreak_buy_price = 0
                        main_account_outbreak_buy_consecutive_days = 0
                        main_account_outbreak_sell_high = 0
                        # 重置爆发模式止盈卖出状态
                        main_account_outbreak_take_profit_active = False
                        main_account_outbreak_take_profit_prev_close = 0
                        # 重置分仓买入状态
                        main_account_outbreak_position_buy_active = False
                        main_account_outbreak_position_buy_count = 0
                        main_account_outbreak_position_buy_prices = []
                        main_account_outbreak_position_buy_pending = False
                        main_account_outbreak_position_buy_trigger_price = 0
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
            
            # 普通卖出后的买回逻辑（防止卖了然后涨了）
            # 当价格高于卖出价格的设定比例时买回
            # 注意：如果处于爆发买入状态，不触发普通买回A（避免冲突）
            if position == 0 and normal_sell_buyback_active and normal_sell_buyback_price > 0 and not main_account_outbreak_buy_active:
                buyback_threshold_price = normal_sell_buyback_price * (1 + NORMAL_SELL_BUYBACK_THRESHOLD)
                if close_price >= buyback_threshold_price:
                    buy_price = close_price
                    new_position = int(cash / buy_price / 100) * 100
                    if new_position >= 100:
                        position = new_position
                        cash -= position * buy_price
                        trade_count += 1
                        action = f"买回A@{buy_price:.2f}(高于卖出价{NORMAL_SELL_BUYBACK_THRESHOLD*100:.0f}%)"
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买回A',
                            'price': buy_price,
                            'shares': position
                        })
                        # 重置买回状态
                        normal_sell_buyback_active = False
                        normal_sell_buyback_price = 0
                        # 重置普通买入A与卖出A的连续N天未创新高卖出状态
                        if ENABLE_NORMAL_NO_NEW_HIGH_SELL:
                            normal_no_new_high_days = 0
                            normal_last_high_price = close_price
                        if holding_start_date is None:
                            holding_start_date = date_str
            
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
                        # 重置跌幅买入待买状态（遇到待买A，结束跌幅买入周期）
                        if drop_buy_delayed_pending:
                            drop_buy_delayed_pending = False
                            drop_buy_delayed_level = -1
                            drop_buy_delayed_type = ''
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
                            # 重置两种买入模式的档位
                        main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        # 重置卖出档位
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        # 重置追买状态
                        main_account_chase_buy_last_price = 0
                        main_account_chase_buy_count = 0
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
                                # 重置两种买入模式的档位
                                main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                                main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                                main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                                # 重置卖出档位
                                main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                                main_account_sell_buy_position = 0
                                main_account_sell_buy_price = 0
                                # 重置追买状态
                                main_account_chase_buy_last_price = 0
                                main_account_chase_buy_count = 0
                            volatility_declining_days = 0
                        # 重置卖出A延迟卖出状态（新买入周期开始）
                            sell_a_delayed_pending = False
                            # 重置普通买入A与卖出A的止盈状态
                            if ENABLE_NORMAL_TAKE_PROFIT:
                                normal_take_profit_levels_triggered = [False] * len(NORMAL_TAKE_PROFIT_LEVELS)
                                normal_take_profit_buy_price = buy_price
                                normal_take_profit_rising_days = 0
                            # 重置普通买入A与卖出A的连续N天未创新高卖出状态
                            if ENABLE_NORMAL_NO_NEW_HIGH_SELL:
                                normal_no_new_high_days = 0
                                normal_last_high_price = close_price
                            # 重置普通卖出后的买回状态（新买入周期开始）
                            normal_sell_buyback_active = False
                            normal_sell_buyback_price = 0
                            if holding_start_date is None:
                                holding_start_date = date_str

            # 延迟买入执行：仅当允许买入时，才从待买A切换到真正买入A
            if ENABLE_BUY_A_DELAYED and position == 0 and buy_a_delayed_pending:
                price_atr_multiplier = 0
                if pd.notna(ma20) and ma20 > 0 and pd.notna(df.loc[i, 'ATR']) and df.loc[i, 'ATR'] > 0:
                    price_atr_multiplier = (close_price - ma20) / df.loc[i, 'ATR']
                can_buy = False
                # 待买A期间如果触发原始卖出A信号，则本轮待买失效并重置，避免跨越原有买卖A周期
                # 在待买A状态下独立检查卖出A条件（波动率>0且降低，且降至前一天阈值以下）
                sell_a_condition_in_pending = False
                if volatility > 0 and is_volatility_declining:
                    volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                    if volatility_ratio <= SELL_RATIO_THRESHOLD:
                        sell_a_condition_in_pending = True
                
                if should_sell or sell_a_condition_in_pending:
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
                        # 重置两种买入模式的档位
                        main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        # 重置卖出档位
                        main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                        # 重置追买状态
                        main_account_chase_buy_last_price = 0
                        main_account_chase_buy_count = 0
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
                        # 重置卖出A延迟卖出状态（新买入周期开始）
                        sell_a_delayed_pending = False
                        # 重置普通买入A与卖出A的止盈状态
                        if ENABLE_NORMAL_TAKE_PROFIT:
                            normal_take_profit_levels_triggered = [False] * len(NORMAL_TAKE_PROFIT_LEVELS)
                            normal_take_profit_buy_price = buy_price
                            normal_take_profit_rising_days = 0
                        # 重置普通买入A与卖出A的连续N天未创新高卖出状态
                        if ENABLE_NORMAL_NO_NEW_HIGH_SELL:
                            normal_no_new_high_days = 0
                            normal_last_high_price = close_price
                        # 重置普通卖出后的买回状态（新买入周期开始）
                        normal_sell_buyback_active = False
                        normal_sell_buyback_price = 0
                        # 重置跌幅买入待买状态（遇到买入A，结束跌幅买入周期）
                        if drop_buy_delayed_pending:
                            drop_buy_delayed_pending = False
                            drop_buy_delayed_level = -1
                            drop_buy_delayed_type = ''
                        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING:
                            main_account_initial_cash = cash
                            main_account_drop_anchor_price = buy_price
                            # 重置两种买入模式的档位
                            main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                            main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                            main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            # 重置卖出档位
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                            main_account_sell_buy_position = 0
                            main_account_sell_buy_price = 0
                            # 重置追买状态
                            main_account_chase_buy_last_price = 0
                            main_account_chase_buy_count = 0
                        volatility_declining_days = 0
                        if holding_start_date is None:
                            holding_start_date = date_str

        # ========================================
        # 爆发买入机制：优先级最高，可在任何情况下触发
        # 触发条件：价ATR倍连续大于阈值时买入（无论是否有持仓、是否在区间交易中）
        # 注意：如果之前是条件E卖出（main_account_outbreak_buy_active=True但持仓为0），
        # 需要等待其他非E的爆发卖出条件（A/B/C/D）触发时才解锁
        # ========================================
        
        # 分仓买入模式：处理后续仓位买入（价格需低于前一次买入价格）
        if ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY and main_account_outbreak_position_buy_active and not main_account_outbreak_position_buy_pending:
            if main_account_outbreak_position_buy_count < MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT and main_account_outbreak_position_buy_count > 0:
                # 检查当前分位
                atr_percentile = row['atr_pct'] if pd.notna(row['atr_pct']) else 0
                
                # 如果分位达到100%，把剩余仓位全部买入
                if atr_percentile >= 1.0:
                    # 计算每仓应该分配的资金
                    cash_per_position = main_account_initial_cash / MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT
                    # 计算已买入的仓位数量对应的资金
                    used_cash = cash_per_position * main_account_outbreak_position_buy_count
                    # 剩余资金 = 初始资金 - 已用资金
                    remaining_cash = main_account_initial_cash - used_cash
                    new_position = int(remaining_cash / close_price / 100) * 100
                    if new_position >= 100 and cash >= new_position * close_price:
                        cost = new_position * close_price
                        cash -= cost
                        # 更新加权平均价格
                        main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
                        main_account_sell_buy_position += new_position
                        main_account_outbreak_position_buy_count = MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT
                        main_account_outbreak_position_buy_prices.append(close_price)
                        trade_count += 1
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': close_price,
                            'shares': new_position,
                            'type': '爆发分仓满仓'
                        })
                        action = f"主账户爆发分仓满仓@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                        # 更新持仓期间最高价
                        if close_price > main_account_holding_high:
                            main_account_holding_high = close_price
                else:
                    # 后续仓位（非第一仓）：只需要价格低于前一次买入价格即可
                    last_buy_price = main_account_outbreak_position_buy_prices[-1] if main_account_outbreak_position_buy_prices else float('inf')
                    if close_price < last_buy_price:
                        position_cash = main_account_initial_cash / MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT
                        new_position = int(position_cash / close_price / 100) * 100
                        if new_position >= 100 and cash >= new_position * close_price:
                            cost = new_position * close_price
                            cash -= cost
                            # 更新加权平均价格
                            main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
                            main_account_sell_buy_position += new_position
                            main_account_outbreak_position_buy_count += 1
                            main_account_outbreak_position_buy_prices.append(close_price)
                            trade_count += 1
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': close_price,
                                'shares': new_position,
                                'type': '爆发分仓买入'
                            })
                            action = f"主账户爆发分仓买入{main_account_outbreak_position_buy_count}/{MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT}@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                            # 更新持仓期间最高价
                            if close_price > main_account_holding_high:
                                main_account_holding_high = close_price
        
        # 爆发卖出E后的买回逻辑
        # 当处于卖出E锁定模式时，价格高于卖出E价格一定百分比则买回
        # 注意：锁定模式下main_account_outbreak_buy_active保持True，但持仓为0
        # 注意：只有在普通持仓为0时才允许卖出E买回（避免与卖出A后的状态冲突）
        if main_account_outbreak_sell_e_active and main_account_outbreak_buy_active and main_account_sell_buy_position == 0 and position == 0:
            # 计算买回阈值价格（卖出E价格 * (1 + 阈值百分比)）
            buyback_threshold_price = main_account_outbreak_sell_e_price * (1 + MAIN_ACCOUNT_OUTBREAK_SELL_E_BUYBACK_THRESHOLD)
            if close_price > buyback_threshold_price:
                # 价格高于卖出E价格，买回
                new_position = int(cash / close_price / 100) * 100
                if new_position >= 100 and cash >= new_position * close_price:
                    cost = new_position * close_price
                    cash -= cost
                    main_account_sell_buy_price = close_price
                    main_account_sell_buy_position += new_position
                    # 保持main_account_outbreak_buy_active = True（已经在爆发买入状态）
                    main_account_outbreak_buy_price = close_price
                    # 初始化持仓期间最高价
                    main_account_holding_high = close_price
                    # 初始化连续未创新高计数
                    main_account_last_high_price = close_price
                    main_account_no_new_high_days = 0
                    # 关闭卖出E锁定模式（恢复正常爆发买入状态）
                    main_account_outbreak_sell_e_active = False
                    main_account_outbreak_sell_e_price = 0
                    trade_count += 1
                    trades.append({
                        'day': day_num,
                        'date': date_str,
                        'action': '买入',
                        'price': close_price,
                        'shares': new_position,
                        'type': '卖出E买回'
                    })
                    action = f"主账户卖出E买回@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                    if holding_start_date is None:
                        holding_start_date = date_str
        
        # 标准爆发买入或启动分仓买入模式
        # 注意：在卖出E锁定模式下（main_account_outbreak_sell_e_active=True）禁止新的爆发买入
        # 注意：只有在普通持仓为0时才允许爆发买入（避免与止盈机制冲突）
        if ENABLE_MAIN_ACCOUNT_OUTBREAK_BUY and position == 0 and not main_account_outbreak_buy_active and not main_account_outbreak_position_buy_active and not main_account_outbreak_sell_e_active:
            current_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
            if current_price_atr > MAIN_ACCOUNT_OUTBREAK_BUY_PRICE_ATR_THRESHOLD:
                main_account_outbreak_buy_consecutive_days += 1
                # 达到连续天数要求才买入
                if main_account_outbreak_buy_consecutive_days >= MAIN_ACCOUNT_OUTBREAK_BUY_CONSECUTIVE_DAYS:
                    # 检查ATR趋势和分位，决定是否启用分仓买入
                    atr_trend = row['atr_pct_trend'] if 'atr_pct_trend' in row else ''
                    atr_percentile = row['atr_pct'] if pd.notna(row['atr_pct']) else 0
                    
                    # 判断是否需要分仓买入：
                    # 1. ATR趋势下降 → 分仓
                    # 2. ATR趋势持平但之前是下降 → 分仓
                    # 3. ATR趋势持平但之前是上升 → 满仓
                    # 4. 分位100%时满仓买入（不分仓）
                    need_position_buy = False
                    if ENABLE_MAIN_ACCOUNT_OUTBREAK_POSITION_BUY and atr_percentile > MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_PERCENTILE_THRESHOLD and atr_percentile < 1.0:
                        if atr_trend == '↓':
                            # 趋势下降 → 分仓
                            need_position_buy = True
                        elif atr_trend == '→':
                            # 趋势持平，需要判断之前是上升还是下降
                            # 检查前一天的MA5变化
                            if i >= 2 and pd.notna(df.loc[i-1, 'atr_pct_ma5']) and pd.notna(df.loc[i-2, 'atr_pct_ma5']):
                                prev_change = df.loc[i-1, 'atr_pct_ma5'] - df.loc[i-2, 'atr_pct_ma5']
                                if prev_change < -ATR_PERCENTILE_TREND_THRESHOLD:
                                    # 之前是下降 → 分仓
                                    need_position_buy = True
                                # 否则（之前是上升或持平）→ 满仓，need_position_buy保持False
                            else:
                                # 数据不足，默认满仓
                                need_position_buy = False
                        # atr_trend == '↑' 时 need_position_buy保持False（满仓）
                    
                    if need_position_buy:
                        # 启动分仓买入模式：先买入第一仓（标准爆发买入点）
                        position_cash = cash / MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT
                        new_position = int(position_cash / close_price / 100) * 100
                        if new_position >= 100 and cash >= new_position * close_price:
                            cost = new_position * close_price
                            cash -= cost
                            # 更新加权平均价格（累加持仓，不是覆盖）
                            if main_account_sell_buy_position > 0:
                                main_account_sell_buy_price = (main_account_sell_buy_price * main_account_sell_buy_position + close_price * new_position) / (main_account_sell_buy_position + new_position)
                            else:
                                main_account_sell_buy_price = close_price
                            main_account_sell_buy_position += new_position
                            main_account_outbreak_position_buy_count = 1
                            main_account_outbreak_position_buy_prices.append(close_price)
                            main_account_outbreak_position_buy_active = True
                            main_account_outbreak_buy_active = True
                            main_account_outbreak_buy_price = close_price
                            # 记录分仓买入的初始资金（用于后续满仓计算）
                            main_account_initial_cash = cash + cost
                            # 初始化N日最高价为当前N日最高
                            high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                            main_account_outbreak_sell_high = row[high_col] if pd.notna(row[high_col]) else close_price
                            # 初始化持仓期间最高价为买入价格
                            main_account_holding_high = close_price
                            # 初始化连续未创新高计数
                            main_account_last_high_price = close_price
                            main_account_no_new_high_days = 0
                            # 重置爆发模式止盈卖出状态
                            main_account_outbreak_take_profit_active = False
                            main_account_outbreak_take_profit_prev_close = 0
                            trade_count += 1
                            trades.append({
                                'day': day_num,
                                'date': date_str,
                                'action': '买入',
                                'price': close_price,
                                'shares': new_position,
                                'type': '爆发分仓买入'
                            })
                            action = f"主账户爆发分仓买入1/{MAIN_ACCOUNT_OUTBREAK_POSITION_BUY_COUNT}@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                            if holding_start_date is None:
                                holding_start_date = date_str
                            # 重置跌幅买入待买状态（遇到分仓买入，结束跌幅买入周期）
                            if drop_buy_delayed_pending:
                                drop_buy_delayed_pending = False
                                drop_buy_delayed_level = -1
                                drop_buy_delayed_type = ''
                        main_account_outbreak_buy_consecutive_days = 0
                    else:
                        # 标准全仓买入
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
                            # 初始化连续未创新高计数
                            main_account_last_high_price = close_price
                            main_account_no_new_high_days = 0
                            # 重置爆发模式止盈卖出状态
                            main_account_outbreak_take_profit_active = False
                            main_account_outbreak_take_profit_prev_close = 0
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
                            # 重置跌幅买入待买状态（遇到爆发买入，结束跌幅买入周期）
                            if drop_buy_delayed_pending:
                                drop_buy_delayed_pending = False
                                drop_buy_delayed_level = -1
                                drop_buy_delayed_type = ''
            else:
                # 不满足条件，重置计数器
                main_account_outbreak_buy_consecutive_days = 0

        if ENABLE_MAIN_ACCOUNT_SELL_BUY_TRADING and position == 0 and (not buy_a_delayed_pending) and main_account_drop_anchor_price > 0:
            drop_anchor_price = main_account_drop_anchor_price
            price_drop_pct = (close_price - drop_anchor_price) / drop_anchor_price if drop_anchor_price > 0 else 0
            
            # 标记当天是否有买入操作
            has_buy_today = False
            
            # 获取当前ATR趋势
            atr_trend = row['atr_pct_trend'] if 'atr_pct_trend' in row else ''

            if close_price < ma20 and drop_anchor_price > 0 and not has_buy_today:
                executed_drop_levels = []
                
                # 根据开关选择买入触发方式
                if ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                    # 基于价ATR倍数的买入
                    # 计算价ATR倍数
                    price_atr_multiplier = 0
                    if pd.notna(ma20) and ma20 > 0 and pd.notna(df.loc[i, 'ATR']) and df.loc[i, 'ATR'] > 0:
                        price_atr_multiplier = (close_price - ma20) / df.loc[i, 'ATR']
                    
                    # ========================================
                    # 模式1: 基于价ATR倍数的买入（波动率条件触发）
                    # ========================================
                    for drop_idx, level in enumerate(MAIN_ACCOUNT_BUY_ATR_LEVELS):
                        if main_account_sell_buy_levels_triggered_atr[drop_idx]:
                            continue
                        # 基于价ATR倍数的触发条件（互斥区间）
                        # 买1: -3 < 价ATR倍 <= -2
                        # 买2: -4 < 价ATR倍 <= -3
                        # 买3: 价ATR倍 <= -4
                        condition_met = False
                        if drop_idx == 0 and (MAIN_ACCOUNT_BUY_ATR_LEVELS[1] < price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[0]):
                            condition_met = True
                        elif drop_idx == 1 and (MAIN_ACCOUNT_BUY_ATR_LEVELS[2] < price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[1]):
                            condition_met = True
                        elif drop_idx == 2 and price_atr_multiplier <= MAIN_ACCOUNT_BUY_ATR_LEVELS[2]:
                            condition_met = True
                        
                        if condition_met:
                            # 条件满足，增加连续天数计数
                            main_account_sell_buy_levels_consecutive_days[drop_idx] += 1
                        else:
                            # 条件不满足，重置连续天数计数
                            main_account_sell_buy_levels_consecutive_days[drop_idx] = 0
                            continue
                        
                        # 检查是否达到连续天数要求
                        if main_account_sell_buy_levels_consecutive_days[drop_idx] < MAIN_ACCOUNT_BUY_ATR_CONSECUTIVE_DAYS[drop_idx]:
                            continue
                        
                        # 检查趋势：如果趋势下降，标记待买；如果趋势变平或变高，执行买入
                        if atr_trend == '↓':
                            # 趋势下降，标记待买
                            if not drop_buy_delayed_pending:
                                drop_buy_delayed_pending = True
                                drop_buy_delayed_level = drop_idx
                                drop_buy_delayed_type = 'ATR'
                                action = f"主账户待跌幅买{drop_idx + 1}@{close_price:.2f}(趋势下降)"
                            continue
                        
                        # 趋势变平或变高，执行买入
                        ratio = MAIN_ACCOUNT_BUY_ATR_RATIOS[drop_idx]
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
                            'level': drop_idx + 1,
                            'type': 'ATR买入'
                        })
                        main_account_sell_buy_levels_triggered_atr[drop_idx] = True
                        executed_drop_levels.append(f"ATR买{drop_idx + 1}")
                        # 记录持仓开始日期（区间交易首次买入）
                        if holding_start_date is None:
                            holding_start_date = date_str
                        # 重置待买状态
                        if drop_buy_delayed_pending and drop_buy_delayed_level == drop_idx and drop_buy_delayed_type == 'ATR':
                            drop_buy_delayed_pending = False
                            drop_buy_delayed_level = -1
                            drop_buy_delayed_type = ''
                
                # ========================================
                # 模式2: 基于跌幅的买入（低比例补跌）
                # ========================================
                for drop_idx, level in enumerate(MAIN_ACCOUNT_BUY_LEVELS):
                    if main_account_sell_buy_levels_triggered_drop[drop_idx]:
                        continue
                    if price_drop_pct > level:
                        continue
                    
                    # 检查趋势：如果趋势下降，标记待买；如果趋势变平或变高，执行买入
                    if atr_trend == '↓':
                        # 趋势下降，标记待买
                        if not drop_buy_delayed_pending:
                            drop_buy_delayed_pending = True
                            drop_buy_delayed_level = drop_idx
                            drop_buy_delayed_type = 'DROP'
                            action = f"主账户待跌幅买{drop_idx + 1}@{close_price:.2f}(趋势下降)"
                        continue

                    ratio = MAIN_ACCOUNT_BUY_DROP_RATIOS[drop_idx]
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
                        'level': drop_idx + 1,
                        'type': '跌幅买入'
                    })
                    main_account_sell_buy_levels_triggered_drop[drop_idx] = True
                    executed_drop_levels.append(f"跌幅买{drop_idx + 1}")
                    # 记录持仓开始日期（区间交易首次买入）
                    if holding_start_date is None:
                        holding_start_date = date_str
                    # 重置待买状态
                    if drop_buy_delayed_pending and drop_buy_delayed_level == drop_idx and drop_buy_delayed_type == 'DROP':
                        drop_buy_delayed_pending = False
                        drop_buy_delayed_level = -1
                        drop_buy_delayed_type = ''

                if executed_drop_levels:
                    drop_levels_str = ','.join(executed_drop_levels)
                    action = f"主账户{drop_levels_str}@{close_price:.2f} 持仓{main_account_sell_buy_position}"
                    has_buy_today = True
                    # 重置两种卖出档位标记
                    main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                    # 记录上一次跌幅买入价格，用于追买机制
                    main_account_chase_buy_last_price = close_price
                    main_account_chase_buy_count = 0
                
                # ========================================
                # 处理待跌幅买入：如果趋势变平或变高，执行之前标记的待买
                # ========================================
                if drop_buy_delayed_pending and atr_trend != '↓' and not has_buy_today:
                    # 趋势变平或变高，执行待买
                    if drop_buy_delayed_type == 'ATR' and ENABLE_MAIN_ACCOUNT_BUY_BY_ATR:
                        drop_idx = drop_buy_delayed_level
                        if drop_idx >= 0 and not main_account_sell_buy_levels_triggered_atr[drop_idx]:
                            ratio = MAIN_ACCOUNT_BUY_ATR_RATIOS[drop_idx]
                            buy_amount = main_account_initial_cash * ratio
                            buy_amount = min(buy_amount, cash)
                            new_position = int(buy_amount / close_price / 100) * 100
                            if new_position >= 100 and cash >= new_position * close_price:
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
                                    'level': drop_idx + 1,
                                    'type': 'ATR买入'
                                })
                                main_account_sell_buy_levels_triggered_atr[drop_idx] = True
                                action = f"主账户ATR买{drop_idx + 1}@{close_price:.2f}(待买执行) 持仓{main_account_sell_buy_position}"
                                has_buy_today = True
                                # 重置卖出档位
                                main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                    elif drop_buy_delayed_type == 'DROP':
                        drop_idx = drop_buy_delayed_level
                        if drop_idx >= 0 and not main_account_sell_buy_levels_triggered_drop[drop_idx]:
                            ratio = MAIN_ACCOUNT_BUY_DROP_RATIOS[drop_idx]
                            buy_amount = main_account_initial_cash * ratio
                            buy_amount = min(buy_amount, cash)
                            new_position = int(buy_amount / close_price / 100) * 100
                            if new_position >= 100 and cash >= new_position * close_price:
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
                                    'level': drop_idx + 1,
                                    'type': '跌幅买入'
                                })
                                main_account_sell_buy_levels_triggered_drop[drop_idx] = True
                                action = f"主账户跌幅买{drop_idx + 1}@{close_price:.2f}(待买执行) 持仓{main_account_sell_buy_position}"
                                has_buy_today = True
                                # 重置卖出档位
                                main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                                # 记录上一次跌幅买入价格，用于追买机制
                                main_account_chase_buy_last_price = close_price
                                main_account_chase_buy_count = 0
                    # 重置待买状态
                    drop_buy_delayed_pending = False
                    drop_buy_delayed_level = -1
                    drop_buy_delayed_type = ''

            # ========================================
            # 追买机制：基于跌幅买入的追加买入
            # 逻辑：价格每相对于上一次跌幅买入价格降低一定幅度，就继续买入
            # 注意：如果价格趋势向下（close_trend == '↓'），则不追买，等待下跌结束
            # ========================================
            # 获取价格趋势
            close_trend = row['close_trend'] if pd.notna(row['close_trend']) else ''
            
            if (ENABLE_MAIN_ACCOUNT_CHASE_BUY and 
                main_account_sell_buy_position > 0 and 
                main_account_chase_buy_last_price > 0 and 
                not has_buy_today and
                main_account_chase_buy_count < MAIN_ACCOUNT_CHASE_BUY_MAX_COUNT and
                close_trend != '↓'):  # 价格趋势向下时不追买，等待下跌结束
                
                # 计算当前价格相对于上一次买入价格的跌幅
                price_drop_from_last = (main_account_chase_buy_last_price - close_price) / main_account_chase_buy_last_price
                
                # 如果跌幅达到追买阈值，执行追买
                if price_drop_from_last >= MAIN_ACCOUNT_CHASE_BUY_PRICE_DROP:
                    buy_amount = main_account_initial_cash * MAIN_ACCOUNT_CHASE_BUY_RATIO
                    buy_amount = min(buy_amount, cash)
                    new_position = int(buy_amount / close_price / 100) * 100
                    
                    if new_position >= 100 and cash >= new_position * close_price:
                        cost = new_position * close_price
                        cash -= cost
                        # 更新总持仓的加权平均价格
                        main_account_sell_buy_price = (
                            main_account_sell_buy_price * main_account_sell_buy_position
                            + close_price * new_position
                        ) / (main_account_sell_buy_position + new_position)
                        main_account_sell_buy_position += new_position
                        trade_count += 1
                        main_account_chase_buy_count += 1
                        # 更新上一次买入价格（用于下一次追买计算）
                        main_account_chase_buy_last_price = close_price
                        trades.append({
                            'day': day_num,
                            'date': date_str,
                            'action': '买入',
                            'price': close_price,
                            'shares': new_position,
                            'level': main_account_chase_buy_count,
                            'type': '追买'
                        })
                        action = f"主账户追买{main_account_chase_buy_count}@{close_price:.2f}(较上次跌{price_drop_from_last*100:.1f}%) 持仓{main_account_sell_buy_position}"
                        has_buy_today = True

            # 在卖出E锁定模式下，即使持仓为0也要检测解锁条件
            # 需要在持仓检查之前执行，确保锁定模式能正常解锁
            if main_account_outbreak_sell_e_active and main_account_outbreak_buy_active:
                # 检测条件A/B/C/D用于解锁
                high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
                current_high = row[high_col] if pd.notna(row[high_col]) else close_price
                
                if current_high > main_account_outbreak_sell_high:
                    main_account_outbreak_sell_high = current_high
                
                if main_account_outbreak_sell_high > 0:
                    high_drop_pct = (main_account_outbreak_sell_high - current_high) / main_account_outbreak_sell_high
                    condition_a_unlock = high_drop_pct > MAIN_ACCOUNT_OUTBREAK_SELL_THRESHOLD
                    price_drop_pct = (close_price - current_high) / current_high if current_high > 0 else 0
                    condition_b_unlock = price_drop_pct < MAIN_ACCOUNT_OUTBREAK_SELL_DROP_THRESHOLD
                    prev_close = df.iloc[i-1]['收盘'] if i > 0 else close_price
                    condition_c_unlock = close_price < prev_close * MAIN_ACCOUNT_OUTBREAK_SELL_PREV_DAY_RATIO
                    if close_price > main_account_holding_high:
                        main_account_holding_high = close_price
                    holding_high_drop_pct = (main_account_holding_high - close_price) / main_account_holding_high if main_account_holding_high > 0 else 0
                    condition_d_unlock = holding_high_drop_pct > MAIN_ACCOUNT_OUTBREAK_SELL_HOLDING_HIGH_THRESHOLD
                    
                    if condition_a_unlock or condition_b_unlock or condition_c_unlock or condition_d_unlock:
                        # 解锁并复位所有爆发状态
                        main_account_outbreak_sell_e_active = False
                        main_account_outbreak_sell_e_price = 0
                        main_account_outbreak_buy_active = False
                        main_account_outbreak_buy_price = 0
                        main_account_outbreak_buy_consecutive_days = 0
                        main_account_outbreak_sell_high = 0
                        main_account_outbreak_take_profit_active = False
                        main_account_outbreak_take_profit_prev_close = 0
                        main_account_outbreak_position_buy_active = False
                        main_account_outbreak_position_buy_count = 0
                        main_account_outbreak_position_buy_prices = []
                        main_account_outbreak_position_buy_pending = False
                        main_account_outbreak_position_buy_trigger_price = 0
                        unlock_reasons = []
                        if condition_a_unlock:
                            unlock_reasons.append('A')
                        if condition_b_unlock:
                            unlock_reasons.append('B')
                        if condition_c_unlock:
                            unlock_reasons.append('C')
                        if condition_d_unlock:
                            unlock_reasons.append('D')
                        action = f"主账户爆发解锁({'+'.join(unlock_reasons)})"
            
            # 只有当当天没有买入操作时，才执行卖出
            if main_account_sell_buy_position > 0 and main_account_sell_buy_price > 0 and not has_buy_today:
                # 检查是否触发爆发卖出
                stop_loss_triggered = False
                take_profit_triggered = False  # 止盈卖出触发标记
                take_profit_sell_shares = 0  # 止盈卖出股数
                take_profit_levels_triggered = []  # 触发的止盈档位
                
                # 爆发买入锁定：当爆发买入激活时，只检查爆发卖出条件和止盈卖出条件，跳过其他卖出机制
                if main_account_outbreak_buy_active:
                    current_price_atr = row['价ATR倍'] if pd.notna(row['价ATR倍']) else 0
                    
                    # ========================================
                    # 爆发模式止盈卖出逻辑（新逻辑）
                    # 逻辑：
                    # 1. 当分位为100%且趋势不是上升时，激活止盈模式
                    # 2. 第二天如果价格低于前一天价格的阈值（默认4%），则卖出10%
                    # 3. 反复如此直到到达爆发卖出点
                    # ========================================
                    if ENABLE_MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT:
                        close_trend = row['close_trend'] if pd.notna(row['close_trend']) else ''
                        atr_pct = row['atr_pct'] if pd.notna(row['atr_pct']) else 0
                        
                        # 检查是否满足止盈模式激活条件：分位100%且趋势非上升
                        if not main_account_outbreak_take_profit_active:
                            if atr_pct >= 1.0 and close_trend != '↑':
                                # 激活止盈模式，记录前一天收盘价
                                main_account_outbreak_take_profit_active = True
                                main_account_outbreak_take_profit_prev_close = close_price
                        else:
                            # 已处于止盈模式，检查是否满足卖出条件
                            if main_account_outbreak_take_profit_prev_close > 0:
                                # 计算价格跌幅
                                price_drop_pct = (main_account_outbreak_take_profit_prev_close - close_price) / main_account_outbreak_take_profit_prev_close
                                
                                if price_drop_pct >= MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT_PRICE_DROP_THRESHOLD:
                                    # 价格跌幅超过阈值，执行止盈卖出
                                    take_profit_sell_shares = int(main_account_sell_buy_position * MAIN_ACCOUNT_OUTBREAK_TAKE_PROFIT_RATIO)
                                    take_profit_sell_shares = min(take_profit_sell_shares, main_account_sell_buy_position)
                                    if take_profit_sell_shares > 0:
                                        take_profit_triggered = True
                                        take_profit_levels_triggered.append(0)
                                
                                # 更新前一天收盘价（无论是否卖出都更新）
                                main_account_outbreak_take_profit_prev_close = close_price
                            
                            # 检查是否继续满足止盈模式条件（分位100%且趋势非上升）
                            if atr_pct < 1.0 or close_trend == '↑':
                                # 不再满足条件，退出止盈模式
                                main_account_outbreak_take_profit_active = False
                                main_account_outbreak_take_profit_prev_close = 0
                    
                    # ========================================
                    # 爆发模式止损卖出逻辑（原有逻辑）
                    # 注意：无论止盈是否触发，都需要检查止损条件，因为止盈只是卖出部分仓位
                    # ========================================
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

                        # 条件E：连续N天未创新高卖出（替代跌破MA20）
                        condition_e = False
                        if MAIN_ACCOUNT_OUTBREAK_SELL_ENABLE_NO_NEW_HIGH_CONDITION:
                            condition_e = main_account_no_new_high_days >= MAIN_ACCOUNT_OUTBREAK_SELL_NO_NEW_HIGH_DAYS

                        # 检查是否触发了A/B/C/D条件（非纯E卖出）
                        # 如果处于卖出E锁定模式且触发了其他爆发卖出条件，则立即解锁并复位所有状态
                        # 注意：解锁逻辑已在持仓检查之前执行，这里保留用于兼容原有逻辑
                        if main_account_outbreak_sell_e_active and (condition_a or condition_b or condition_c or condition_d):
                            # 解锁并复位所有爆发状态，结束本轮循环
                            main_account_outbreak_sell_e_active = False
                            main_account_outbreak_sell_e_price = 0
                            main_account_outbreak_buy_active = False
                            main_account_outbreak_buy_price = 0
                            main_account_outbreak_buy_consecutive_days = 0
                            main_account_outbreak_sell_high = 0
                            main_account_outbreak_take_profit_active = False
                            main_account_outbreak_take_profit_prev_close = 0
                            main_account_outbreak_position_buy_active = False
                            main_account_outbreak_position_buy_count = 0
                            main_account_outbreak_position_buy_prices = []
                            main_account_outbreak_position_buy_pending = False
                            main_account_outbreak_position_buy_trigger_price = 0
                            # 记录解锁操作
                            unlock_reasons = []
                            if condition_a:
                                unlock_reasons.append('A')
                            if condition_b:
                                unlock_reasons.append('B')
                            if condition_c:
                                unlock_reasons.append('C')
                            if condition_d:
                                unlock_reasons.append('D')
                            action = f"主账户爆发解锁({'+'.join(unlock_reasons)})"
                        
                        # 任一条件满足即触发卖出，并记录触发条件
                        # 优先级：A/B/C/D条件优先于E条件
                        # 如果A/B/C/D任一条件满足，优先记录这些条件（不记录E，即使E也满足）
                        if condition_a or condition_b or condition_c or condition_d or condition_e:
                            stop_loss_triggered = True
                            # 记录触发的条件（可能有多个）
                            reasons = []
                            # 优先检查A/B/C/D条件
                            if condition_a:
                                reasons.append('A')
                            if condition_b:
                                reasons.append('B')
                            if condition_c:
                                reasons.append('C')
                            if condition_d:
                                reasons.append('D')
                            # 只有当A/B/C/D都不满足时，才记录E
                            if not reasons and condition_e:
                                reasons.append('E')
                            outbreak_sell_reason = '+'.join(reasons)

                current_atr = row['atr'] if pd.notna(row['atr']) else 0
                # 爆发买入锁定：当爆发买入激活时，只允许多止盈卖出和止损卖出，跳过追跌卖档位逻辑
                can_evaluate_sell = stop_loss_triggered or take_profit_triggered or (not main_account_outbreak_buy_active)

                if can_evaluate_sell:
                    # 计算基于MA20的价ATR倍数
                    ma20_price_atr_multiplier = 0
                    if pd.notna(ma20) and ma20 > 0 and current_atr > 0:
                        ma20_price_atr_multiplier = (close_price - ma20) / current_atr

                    # ========================================
                    # 卖出模式：只使用ATR卖出模式
                    # ========================================
                    max_triggered_level = -1
                    newly_triggered_levels = []
                    
                    # 只有在非爆发买入模式下，才计算卖出档位
                    if not main_account_outbreak_buy_active:
                        # 计算ATR卖出档位
                        for sell_idx in range(len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS) - 1, -1, -1):
                            if ma20_price_atr_multiplier >= MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS[sell_idx]:
                                max_triggered_level = sell_idx
                                break
                        
                        if max_triggered_level >= 0:
                            trigger_cap = min(max_triggered_level + 1, len(main_account_sell_sell_levels_triggered))
                            for idx in range(trigger_cap):
                                if not main_account_sell_sell_levels_triggered[idx]:
                                    main_account_sell_sell_levels_triggered[idx] = True
                                    newly_triggered_levels.append(idx)

                    trade_level = 0
                    # reset rise/drop independent prices
                    if stop_loss_triggered:
                        # 爆发买入止损卖出所有仓位
                        sell_shares = main_account_sell_buy_position
                        sell_shares = max(sell_shares, 0)
                    elif take_profit_triggered:
                        # 爆发买入止盈卖出
                        sell_shares = take_profit_sell_shares
                        sell_shares = max(sell_shares, 0)
                    elif newly_triggered_levels:
                        # 计算卖出比例
                        total_ratio = sum(MAIN_ACCOUNT_SELL_RATIOS[idx] for idx in newly_triggered_levels if idx < len(MAIN_ACCOUNT_SELL_RATIOS))
                        sell_shares = int(main_account_sell_buy_position * total_ratio)
                        sell_shares = min(sell_shares, main_account_sell_buy_position)
                        trade_level = newly_triggered_levels[-1] + 1
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
                        elif take_profit_triggered:
                            # 构建止盈卖出档位描述
                            if len(take_profit_levels_triggered) == 1:
                                triggered_levels = f"爆发止盈档{take_profit_levels_triggered[0]+1}"
                            else:
                                triggered_levels = f"爆发止盈档{take_profit_levels_triggered[0]+1}-{take_profit_levels_triggered[-1]+1}"
                        else:
                            # 构建卖出档位描述
                            triggered_levels = ",".join([f"卖{i+1}" for i in newly_triggered_levels])
                        remaining_position = main_account_sell_buy_position
                        action = f"主账户{triggered_levels}@{close_price:.2f} 持仓{remaining_position}"
                        # 卖出后重置买入档位，允许在更低价格继续追跌买入
                        # 重置两种买入模式的档位
                        main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                        main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                        if main_account_sell_buy_position == 0:
                            main_account_sell_buy_price = 0
                            # 重置两种买入模式的档位
                            main_account_sell_buy_levels_triggered_atr = [False] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                            main_account_sell_buy_levels_consecutive_days = [0] * len(MAIN_ACCOUNT_BUY_ATR_LEVELS)
                            main_account_sell_buy_levels_triggered_drop = [False] * len(MAIN_ACCOUNT_BUY_LEVELS)
                            # 重置卖出档位
                            main_account_sell_sell_levels_triggered = [False] * len(MAIN_ACCOUNT_SELL_ATR_MULTIPLIERS)
                            # reset rise/drop independent prices
                            holding_start_date = None
                            # 区间交易全部卖出后，设置锚定价格以便后续继续区间交易买入
                            main_account_drop_anchor_price = close_price
                            # 只有当仓位全部卖出时，才重置爆发买入状态
                            # 检查是否是条件E卖出（包含E，可能有其他条件混合）
                            if stop_loss_triggered and 'E' in outbreak_sell_reason:
                                # 条件E卖出，启动锁定模式
                                # 注意：不重置爆发买入状态，保持main_account_outbreak_buy_active=True
                                # 也不重置main_account_outbreak_sell_high，以便继续检测A/B/C/D条件进行解锁
                                main_account_outbreak_sell_e_active = True
                                main_account_outbreak_sell_e_price = close_price
                                # 保持爆发状态不变，只是持仓变为0
                                # 这样锁定期间只能进行买回E操作
                            else:
                                # 非E卖出，重置所有爆发状态
                                main_account_outbreak_buy_active = False
                                main_account_outbreak_buy_price = 0
                                main_account_outbreak_buy_consecutive_days = 0
                                main_account_outbreak_sell_high = 0  # 重置N日最高价
                                # 重置爆发模式止盈卖出状态
                                main_account_outbreak_take_profit_active = False
                                main_account_outbreak_take_profit_prev_close = 0
                                # 重置分仓买入状态
                                main_account_outbreak_position_buy_active = False
                                main_account_outbreak_position_buy_count = 0
                                main_account_outbreak_position_buy_prices = []
                                main_account_outbreak_position_buy_pending = False
                                main_account_outbreak_position_buy_trigger_price = 0
                            # 重置连续未创新高计数
                            main_account_no_new_high_days = 0
                            main_account_last_high_price = 0

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
        atr_str = f"{row['atr']:.2f}" if pd.notna(row['atr']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        price_atr_ratio_str = f"{row['价ATR倍']:.2f}" if pd.notna(row['价ATR倍']) else "N/A"
        # atr_pct列（ATR分位数，显示为百分比）
        atr_pct_str = f"{row['atr_pct']*100:.1f}%" if pd.notna(row['atr_pct']) else "N/A"
        # atr_pct趋势列
        atr_pct_trend_str = row['atr_pct_trend'] if pd.notna(row['atr_pct_trend']) else ""
        # 价格分位列（显示为百分比，周期可配置）
        price_pct_str = f"{row['price_pct']*100:.1f}%" if pd.notna(row['price_pct']) else "N/A"
        # 计算5日ATR平均
        five_day_atr_avg_str = f"{row['5日价ATR平均']:.2f}" if pd.notna(row['5日价ATR平均']) else "N/A"
        # 10日最低ATR倍数
        ten_day_low_atr_str = f"{row['10日最低价ATR倍数']:.2f}" if pd.notna(row['10日最低价ATR倍数']) else "N/A"
        # N日最高价（可配置周期）
        high_col = f'{MAIN_ACCOUNT_OUTBREAK_SELL_HIGH_DAYS}日最高'
        n_day_high_str = f"{row[high_col]:.2f}" if pd.notna(row[high_col]) else "N/A"
        # 持仓期间最高价（适用于所有持仓）
        total_position = position + main_account_sell_buy_position
        holding_high_str = f"{main_account_holding_high:.2f}" if total_position > 0 and main_account_holding_high > 0 else "N/A"

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {atr_str:>8} {volatility_str:>8} {price_atr_ratio_str:>8} {atr_pct_str:>8} {atr_pct_trend_str:>6} {price_pct_str:>8} {five_day_atr_avg_str:>8} {ten_day_low_atr_str:>12} {n_day_high_str:>8} {holding_high_str:>8}   {action:<30} {position_str:>8} {market_value:>12,.2f})")
    
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
        # 用于跟踪持仓成本的变量
        current_position = 0
        current_avg_cost = 0.0
        
        for idx, trade in enumerate(trades, 1):
            profit_str = f"{trade.get('profit', 0):,.2f}" if 'profit' in trade else "-"
            # 计算盈亏百分比
            if 'profit' in trade and trade['action'] == '卖出':
                # 使用累计的平均持仓成本计算盈利百分比
                if current_position > 0 and current_avg_cost > 0:
                    total_cost = current_avg_cost * trade['shares']
                    profit_pct = (trade.get('profit', 0) / total_cost) * 100
                    profit_pct_str = f"{profit_pct:+.2f}%"
                else:
                    profit_pct_str = "-"
                # 卖出后减少持仓
                current_position -= trade['shares']
                if current_position <= 0:
                    current_position = 0
                    current_avg_cost = 0.0
            else:
                profit_pct_str = "-"
                # 买入时更新平均持仓成本
                if trade['action'] == '买入' and trade['shares'] > 0:
                    if current_position == 0:
                        current_avg_cost = trade['price']
                        current_position = trade['shares']
                    else:
                        # 加权平均计算新的持仓成本
                        total_cost = current_avg_cost * current_position + trade['price'] * trade['shares']
                        current_position += trade['shares']
                        current_avg_cost = total_cost / current_position
            log_print(f"{idx:<6} {trade['date']:<12} {trade['action']:<6} {trade['price']:>10.2f} {trade['shares']:>10} {profit_str:>12} {profit_pct_str:>8}")

    log_print(f"{'='*175}")

    # 预测第二天卖出触发价格（只在有持仓时计算）
    if position > 0:
        last_row = df.iloc[-1]
        last_close = last_row['收盘']
        last_ma20 = last_row['ma20']
        last_atr = last_row['atr']
        last_volatility = last_row['波动率']
        
        log_print(f"\n【第二天卖出价格预测 - 基于当前价格{last_close:.2f}】")
        log_print(f"预测日期: {df.iloc[-1]['date'].strftime('%Y-%m-%d') if hasattr(df.iloc[-1]['date'], 'strftime') else str(df.iloc[-1]['date'])[:10]}")
        log_print(f"当前持仓: {position}股")
        
        # 收集所有可能的卖出触发价格
        sell_triggers = []
        
        # 波动率比率卖出
        if pd.notna(last_volatility) and pd.notna(last_atr) and last_atr > 0 and len(df) >= 5:
            ma20_t_minus_4 = df.iloc[-5]['ma20'] if pd.notna(df.iloc[-5]['ma20']) else last_ma20
            
            # 计算波动率比率卖出的触发价格
            # 如果波动率 > 0 且下一天波动率降低至 SELL_RATIO_THRESHOLD 以下
            if last_volatility > 0:
                target_volatility = last_volatility * SELL_RATIO_THRESHOLD
                target_ma20_change = target_volatility * last_atr
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


