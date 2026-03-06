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
ENABLE_CONDITION_C = True  # 设置为False可关闭条件C买入

# 买入条件全局参数
BUY_DECLINE_DAYS_REQUIRED = 2  # 波动率连续向0靠近所需天数（条件A）

# 条件C参数
BUY_CONDITION_C_DAYS = 4       # 波动率>1的连续天数要求
BUY_CONDITION_C_VOL_THRESHOLD = 0.85  # 波动率阈值（用于计数）

# 条件C分仓买入参数
CONDITION_C_POSITION_THRESHOLD = 4  # 价ATR倍阈值，超过此值需要分仓买入（默认3）
CONDITION_C_MA20_PCT_THRESHOLD = -20  # MA20幅%阈值，小于此值（如-10%）需要分仓买入（默认-10%）
CONDITION_C_FIRST_POSITION_RATIO = 1/3 # 第一次买入比例（1/3仓）
CONDITION_C_SECOND_POSITION_RATIO = 1/3  # 第二次买入比例（1/3仓）

# 延迟买入开关
ENABLE_DELAYED_BUY = True  # 设置为True启用延迟买入模式）

# 延迟卖出开关
ENABLE_DELAYED_SELL = True  # 设置为True启用延迟卖出模式

# 持仓期间价格追踪止损开关
ENABLE_STOP_LOSS = True  # 设置为True启用持仓期间价格追踪止损策略
STOP_LOSS_MA20_THRESHOLD = -7 # MA20阈值%，价格低于MA20但在阈值范围内不卖出（默认-5%，即低于MA20 5%以内不卖出）

# 买入A五日最高条件开关
ENABLE_BUY_A_5DAY_HIGH_CHECK = False  # 设置为True启用：买入A时如果收盘价是五日最高则放弃买入


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
    
    # 计算MA20相对于收盘价的幅度百分比
    df['MA20幅度%'] = ((df['ma20'] - df['收盘']) / df['收盘'] * 100).replace([np.inf, -np.inf], np.nan)

    # 计算价格相对ATR的倍数：(收盘价 - MA20) / ATR
    df['价ATR倍'] = ((df['收盘'] - df['ma20']) / df['atr14']).replace([np.inf, -np.inf], np.nan)

    # 计算五日收盘最高价
    df['5日最高'] = df['收盘'].rolling(window=5, min_periods=1).max()

    # 初始化交易变量
    initial_capital = 100000
    cash = initial_capital
    position = 0
    buy_price = 0
    trades = []
    trade_count = 0
    
    # 买入条件计数器
    volatility_declining_days = 0  # 波动率连续向0靠近天数（数值变大）
    prev_volatility = None         # 前一天波动率
    volatility_above_one_days = 0  # 波动率在1以上的连续天数
    is_condition_c_trade = False   # 标记是否为条件C买入的交易
    
    # 延迟买入状态变量
    pending_buy_price = 0          # 待买入价格（记录触发买入条件时的价格）
    pending_buy_condition = ""     # 待买入条件类型（A/B/C）
    is_pending_buy = False         # 是否有待执行的买入
    pending_buy_volatility = 0     # 待买入时的波动率（用于判断第二天是否继续向0逼近）
    
    # 延迟卖出状态变量
    pending_sell_price = 0         # 待卖出价格（记录触发卖出条件时的价格）
    is_pending_sell = False        # 是否有待执行的卖出
    
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
    log_print(f"买入条件C: (波动率>{BUY_CONDITION_C_VOL_THRESHOLD}连续第{BUY_CONDITION_C_DAYS}天) - 分仓买入(价ATR倍>{CONDITION_C_POSITION_THRESHOLD}或MA20幅%<{CONDITION_C_MA20_PCT_THRESHOLD}%时分3批)")
    stop_loss_str = "; 持仓价格追踪止损" if ENABLE_STOP_LOSS else ""
    log_print(f"卖出条件: 波动率>0且降低时，降至前一天{SELL_RATIO_THRESHOLD*100:.0f}%以下则全卖；条件C买入需等>{BUY_CONDITION_C_VOL_THRESHOLD}天数归0才卖{stop_loss_str}")
    if ENABLE_DELAYED_BUY:
        log_print(f"延迟买入: 触发条件后记录买入点，收盘价超过买入点时买入，价格更低时更新买入价")
    if ENABLE_DELAYED_SELL:
        log_print(f"延迟卖出: 触发条件后记录卖出点，收盘价低于卖出点时卖出，价格更高时更新卖出价")
    if ENABLE_STOP_LOSS:
        log_print(f"持仓止损: 买入后第二天收盘价<MA20时启动价格追踪，低于MA20 {abs(STOP_LOSS_MA20_THRESHOLD)}%才卖出，防止震荡")
    log_print(f"{'='*175}\n")

    header = f"{'日':<5} {'日期':<12} {'收盘':>8} {'MA20':>8} {'MA20幅%':>8} {'ATR14':>8} {'波动率':>8} {'波幅%':>8} {'价ATR倍':>8} {'5日最高':>8} {'>1天数':>6} {'操作':<12} {'持仓':>8} {'市值':>12}"
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
        
        action = ""
        
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
                        
                        if is_below_ma20 or is_first_day_above_ma20:
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
                                    # 跌破最高价，卖出
                                    is_stop_loss_triggered = True
                                    sell_reason = "趋势上涨破高"
                            elif price_trend_direction == 'down':
                                # 下跌趋势：更新最低价
                                if close_price < lowest_price_since_buy:
                                    lowest_price_since_buy = close_price
                                    # 继续下跌，保持持仓等待虚拟仓卖出
            
            # 其他卖出条件（仅在未触发价格追踪止损时检查）
            # 注意：条件C买入的交易也支持价格追踪止损，优先级最高
            if not is_stop_loss_triggered:
                if is_condition_c_trade and position > 0:
                    # 条件C买入的交易：只要volatility_above_one_days归0就卖出（不判断波动率是否降低）
                    if volatility_above_one_days == 0:
                        should_sell = True
                        sell_reason = "C条件"
                elif position > 0 and not is_condition_c_trade:
                    # 普通卖出条件1：波动率>0且降低，且降至前一天97%以下
                    if volatility > 0 and is_volatility_declining:
                        volatility_ratio = volatility / prev_volatility if prev_volatility > 0 else 1.0
                        if volatility_ratio <= SELL_RATIO_THRESHOLD:
                            should_sell = True
                            sell_reason = "比率卖出"
                    
            
            # 卖出逻辑（支持延迟卖出和立即止损）
            if position > 0:
                # 止损卖出：立即执行，不进入待卖出状态
                if is_stop_loss_triggered:
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
                    # 重置计数器
                    volatility_declining_days = 0
                    volatility_above_one_days = 0  # 卖出后重置C条件天数
                    # 重置条件C分仓状态
                    condition_c_position_stage = 0
                    condition_c_prev_price = 0
                    condition_c_prev_ma20_pct = 0
                    # 重置延迟状态
                    is_pending_buy = False
                    pending_buy_price = 0
                    pending_buy_condition = ""
                    is_pending_sell = False
                    pending_sell_price = 0
                    # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                    hold_days = 0
                    # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                    price_trend_direction = None
                
                # 检查是否有待执行的延迟卖出（价格追踪止损不进入此逻辑）
                elif is_pending_sell and ENABLE_DELAYED_SELL:
                    # 如果当天价格高于待卖出价，更新卖出价（取更高的价格）
                    if close_price > pending_sell_price:
                        pending_sell_price = close_price
                        action = f"更新卖价@{pending_sell_price:.2f}"
                    # 如果收盘价低于待卖出价，执行卖出
                    elif close_price <= pending_sell_price:
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
                        # 重置计数器
                        volatility_declining_days = 0
                        volatility_above_one_days = 0  # 卖出后重置C条件天数
                        # 重置条件C分仓状态
                        condition_c_position_stage = 0
                        condition_c_prev_price = 0
                        # 重置延迟状态
                        is_pending_buy = False
                        pending_buy_price = 0
                        pending_buy_condition = ""
                        is_pending_sell = False
                        pending_sell_price = 0
                        # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                        hold_days = 0
                        # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                        price_trend_direction = None

                        # 非止损卖出，同步清空虚拟仓
                        virtual_position = 0
                
                # 正常卖出逻辑（非延迟模式或触发卖出条件时）
                elif should_sell:
                    if ENABLE_DELAYED_SELL and not is_pending_sell:
                        # 延迟卖出模式：记录卖出点但不真正卖出
                        is_pending_sell = True
                        pending_sell_price = close_price
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
                        is_condition_c_trade = False
                        # 卖出后重置计数器
                        volatility_declining_days = 0
                        volatility_above_one_days = 0  # 卖出后重置C条件天数
                        # 重置条件C分仓状态
                        condition_c_position_stage = 0
                        condition_c_prev_price = 0
                        # 重置延迟买入状态
                        is_pending_buy = False
                        pending_buy_price = 0
                        pending_buy_condition = ""
                        # 重置延迟卖出状态
                        is_pending_sell = False
                        pending_sell_price = 0
                        # 重置持仓期间价格追踪变量（但保持highest_price_since_buy用于价格追踪期间的延迟买入）
                        hold_days = 0
                        # 注意：不重置highest_price_since_buy和lowest_price_since_buy，保持价格追踪状态
                        price_trend_direction = None

                        # 非止损卖出，同步清空虚拟仓
                        virtual_position = 0
            
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
                    pending_buy_prev_price = prev_day_close  # 记录前一天价格用于比较
                    action = f"待买入@{pending_buy_price:.2f}(追踪期,反弹)"
            
            # 买入逻辑（只有在没有持仓时才买入）
            if position == 0:
                # 检查是否有待执行的延迟买入（只在价格追踪期间启用）
                if is_pending_buy and ENABLE_DELAYED_BUY:
                    # 价格追踪期间的延迟买入：检查价格是否继续上涨
                    # 当天价格 > 前一天价格（待买入记录时的价格）
                    price_continuing_up = close_price > pending_buy_price
                    
                    if price_continuing_up:
                        # 价格继续上涨，执行买入
                        buy_price = close_price
                        new_position = int(cash / buy_price)
                        if new_position > 0:
                            position = new_position
                            cost = position * buy_price
                            cash -= cost
                            trade_count += 1
                            is_condition_c_trade = False  # 价格追踪期间的买入标记为普通买入
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
                            pending_buy_prev_price = 0
                            # 初始化持仓期间价格追踪变量
                            hold_days = 0
                            highest_price_since_buy = 0
                            lowest_price_since_buy = 0
                            price_trend_direction = None
                            # 同步更新虚拟仓
                            virtual_position = position
                    else:
                        # 价格没有继续上涨，更新待买入价格（取更低的价格）
                        if close_price < pending_buy_price:
                            pending_buy_price = close_price
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
                        
                        # 分仓买入条件：价ATR倍 > 阈值 或 MA20幅% < 阈值（或的关系）
                        need_position_buy = (price_atr_ratio > CONDITION_C_POSITION_THRESHOLD or 
                                            ma20_pct < CONDITION_C_MA20_PCT_THRESHOLD)
                        
                        if need_position_buy:
                            # 第一批买入 1/3
                            new_position = int(cash * CONDITION_C_FIRST_POSITION_RATIO / buy_price)
                            if new_position > 0:
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                is_condition_c_trade = True
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
                                condition_c_position_stage = 3  # 标记为已全仓
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

                    # 条件A买入（全仓）- 主逻辑立即买入，不延迟
                    elif condition_a:
                        # 检查：如果启用五日最高条件检查，且买入当天收盘价是五日最高，则放弃此次买入
                        should_abandon_buy = False
                        if ENABLE_BUY_A_5DAY_HIGH_CHECK:
                            high_5day = row['5日最高'] if pd.notna(row['5日最高']) else 0
                            if close_price >= high_5day:
                                should_abandon_buy = True
                        
                        if should_abandon_buy:
                            # 放弃买入，所有内容复位，当作卖出处理
                            action = f"放弃A@{close_price:.2f}(五日最高)"
                            # 复位所有买入相关状态
                            volatility_declining_days = 0
                            is_pending_buy = False
                            pending_buy_price = 0
                            pending_buy_condition = ""
                            pending_buy_volatility = 0
                        else:
                            buy_price = close_price
                            new_position = int(cash / buy_price)
                            if new_position > 0:
                                position = new_position
                                cost = position * buy_price
                                cash -= cost
                                trade_count += 1
                                is_condition_c_trade = False
                                action = f"买入A@{buy_price:.2f}"
                                trades.append({
                                    'day': day_num,
                                    'date': date_str,
                                    'action': '买入',
                                    'price': buy_price,
                                    'shares': position,
                                    'is_condition_c': False
                                })
                                volatility_declining_days = 0
                                # 初始化持仓期间价格追踪变量
                                hold_days = 0
                                highest_price_since_buy = 0
                                lowest_price_since_buy = 0
                                price_trend_direction = None
                                # 同步更新虚拟仓（完全镜像）
                                virtual_position = position
        
        # 计算当前市值
        market_value = cash + position * close_price if position > 0 else cash
        position_str = f"{position}" if position > 0 else "0"
        
        # 数据显示
        ma20_str = f"{ma20:.2f}" if pd.notna(ma20) else "N/A"
        ma20_pct_str = f"{ma20_pct:.2f}" if pd.notna(ma20_pct) else "N/A"
        atr14_str = f"{row['atr14']:.2f}" if pd.notna(row['atr14']) else "N/A"
        volatility_str = f"{volatility:.2f}" if pd.notna(volatility) else "N/A"
        volatility_pct_str = f"{row['波动率百分比']:.1f}" if pd.notna(row['波动率百分比']) else "N/A"
        price_atr_ratio_str = f"{row['价ATR倍']:.2f}" if pd.notna(row['价ATR倍']) else "N/A"
        high_5day_str = f"{row['5日最高']:.2f}" if pd.notna(row['5日最高']) else "N/A"

        log_print(f"{day_num:<5} {date_str:<12} {close_price:>8.2f} {ma20_str:>8} {ma20_pct_str:>8} {atr14_str:>8} {volatility_str:>8} {volatility_pct_str:>8} {price_atr_ratio_str:>8} {high_5day_str:>8} {volatility_above_one_days:>6} {action:<12} {position_str:>8} {market_value:>12,.2f}")
    
    # 计算最终收益
    final_value = cash + position * df.iloc[-1]['收盘'] if position > 0 else cash
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
    log_print(f"\n条件C交易统计:")
    log_print(f"  买入次数: {condition_c_count}")
    log_print(f"  亏损次数: {condition_c_loss_count}")
    log_print(f"  总盈利: {condition_c_total_profit:,.2f}")

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

    # 写入文件
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
