"""
六状态市场分析策略
- 起始资金10万，分N仓（默认3仓）
- 状态转换时买入/卖出
- 涨幅3%买入下一仓

六状态分析:
- 上升趋势 / 自然回升 / 次级回升
- 下降趋势 / 自然回撤 / 次级回撤
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
from market_state_analyzer import MarketStateAnalyzer, MarketState

# ==================== 策略配置参数 ====================
INITIAL_CAPITAL = 100000  # 起始资金
POSITION_COUNT = 3    # 分仓数量（默认4仓）
ADD_POSITION_THRESHOLD = 0.07  # 加仓阈值，每上涨3%买入下一仓

BREAKOUT_THRESHOLD = 0.02  # 突破阈值3%
BUY_DELAY_RISE_PCT = 0.02  # 所有买点统一延迟买入阈值（相对状态切换点上涨3%）
ENABLE_DELAYED_BUY_MODE = True # 延迟买入开关：True=延迟观察买入，False=状态切换当日直接买入
# 上升趋势→自然回撤跳过卖出开关
# True: 启用跳过卖出逻辑（突破前高超过阈值时跳过卖出）
# False: 禁用跳过卖出逻辑（总是卖出）
ENABLE_SKIP_SELL_ON_BREAKOUT = True


# 趋势转换当天大涨直接买入配置
ENABLE_DIRECT_BUY_ON_BIG_RISE = True  # 是否启用趋势转换当天大涨直接买入
DIRECT_BUY_RISE_THRESHOLD = 0.07      # 趋势转换当天相比前一天涨幅阈值（默认7%）

# 所有上升趋势的特殊卖出配置
ENABLE_UPTREND_SELL = True  # 是否启用所有上升趋势的特殊卖出
UPTREND_BREAKOUT_THRESHOLD = 0.05  # 突破上一轮高点的阈值（默认2%）

# 所有自然回升的特殊卖出配置
ENABLE_NATURAL_RALLY_SELL = False  # 是否启用自然回升的特殊卖出
NATURAL_RALLY_BREAKOUT_THRESHOLD = 0.02  # 突破前一轮自然回升高点的阈值（默认2%）

# 自然回升中超过前低买入配置（下降趋势→自然回升因破前低跳过买入后，在自然回升中超过前低时买入）
ENABLE_NATURAL_RALLY_BREAKOUT_BUY = True  # 是否启用自然回升中超过前低买入
NATURAL_RALLY_BREAKOUT_BUY_THRESHOLD = 0.05  # 超过前低的阈值（默认0%，即价格 >= 前低)

# 所有自然回撤的特殊买入配置
ENABLE_NATURAL_REACTION_BUY = False  # 是否启用自然回撤的特殊买入
NATURAL_REACTION_BREAKOUT_THRESHOLD = 0.02  # 相对前一轮自然回撤低点的阈值（默认2%，即当前低点 >= 前低 * 0.98)

# 所有下降趋势的特殊买入配置
ENABLE_DOWNTREND_BUY = True  # 是否启用所有下降趋势的特殊买入
DOWNTREND_BREAKOUT_THRESHOLD = 0.02  # 相对上一轮低点的阈值（默认5%，即当前低点 >= 前低 * 0.95)
ENABLE_DOWNTREND_BUY_DELAY = True  # 下降趋势结束买入延迟观察开关
DOWNTREND_BUY_DELAY_PCT = 0.02  # 下降趋势结束延迟买入阈值（相对触发点上涨2%）

# 状态转换阈值配置（固定数值：元，不再是百分比）
# 根据股价区间设置6个点对应的固定数值（元），THREE_POINTS自动为SIX_POINTS的一半
# 股价 < 5元：不操作
# 5~15元：SIX_POINTS = 1.2元
# 15~40元：SIX_POINTS = 3元
# 40~80元：SIX_POINTS = 6元
# 80~200元：SIX_POINTS = 12元   ``
SIX_POINTS_CONFIG = {
    (1.5, 3): 0.3,      # 1.5~3元：0.3元
    (3, 5): 0.6,      # 3~5元：0.6元
    (5, 12): 1.3,      # 5~15元：1.2元
    (12, 40): 2.4,     # 15~40元：3元
    (40, 90): 6,     # 40~80元：6元
    (90, 200): 12.0,   # 80~200元：12元
}
MIN_PRICE_TO_OPERATE = 1.5  # 最小操作股价（低于此价格不操作）


def get_six_points_by_price(price: float) -> float:
    """根据股价获取对应的SIX_POINTS数值（元）
    
    Args:
        price: 当前股价
        
    Returns:
        SIX_POINTS数值（元），如果股价不在配置范围内返回None
    """
    if price < MIN_PRICE_TO_OPERATE:
        return None
    
    for (low, high), value in SIX_POINTS_CONFIG.items():
        if low <= price < high:
            return value
    
    # 如果股价超过200元，使用最高档位12元
    if price >= 200:
        return 12.0
    
    return None


def get_three_points_by_price(price: float) -> float:
    """根据股价获取对应的THREE_POINTS数值（元）
    
    THREE_POINTS自动为SIX_POINTS的一半
    
    Args:
        price: 当前股价
        
    Returns:
        THREE_POINTS数值（元），如果股价不在配置范围内返回None
    """
    six_points = get_six_points_by_price(price)
    if six_points is None:
        return None
    return six_points / 2


def get_output_file_path(base_name="out_put.txt"):
    """获取可用的输出文件路径，如果被占用则使用序号递增"""
    if not os.path.exists(base_name):
        return base_name
    
    try:
        with open(base_name, 'w', encoding='utf-8') as f:
            pass
        return base_name
    except (PermissionError, IOError):
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
                if counter > 100:
                    raise Exception("无法找到可用的输出文件路径")


def run_backtest(stock_code: str = STOCK_CODE, outbreak_sell_e_buyback_threshold: float = None, outbreak_position_buy_count: int = None):
    """回测主函数
    
    Args:
        stock_code: 股票代码
        outbreak_sell_e_buyback_threshold: 卖出E后的买回阈值（本策略不使用，为兼容run_all_socks.py接口保留）
        outbreak_position_buy_count: 分仓数量（本策略不使用，为兼容run_all_socks.py接口保留）
    """
    
    # 获取日线数据
    df = get_daily_data(stock_code, days=365 * BACKTEST_YEARS + 400)
    
    if df is None or len(df) < 400:
        print(f"数据不足，需要至少400天数据，当前只有{len(df) if df is not None else 0}天")
        return None
    
    # 准备数据
    df = prepare_stock_data(df)
    
    # 执行回测逻辑
    return _run_backtest_core(stock_code, df)


def prepare_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """准备股票数据，计算N日高低价"""
    df = df.copy()
    
    # 按日期从远到近排序
    df = df.sort_values('date').reset_index(drop=True)

    # 只取最近一年的数据
    days_to_show = 365 * BACKTEST_YEARS
    df = df.tail(days_to_show).reset_index(drop=True)

    return df


def _run_backtest_core(stock_code: str, df: pd.DataFrame):
    """回测核心函数"""
    
    # 获取年份范围
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    
    # 初始化交易变量
    initial_capital = INITIAL_CAPITAL
    cash = initial_capital  # 可用现金
    position = 0  # 持仓数量
    position_cost = 0  # 持仓成本（平均成本）
    position_count = 0  # 已买入仓位数量
    last_buy_price = 0  # 上次买入价格（用于计算加仓）
    invested_capital = 0  # 已投入资金
    fixed_position_value = 0  # 固定的每仓金额（买入第一仓时确定）
    
    # 交易记录
    trades = []
    
    # 初始化市场状态分析器（传入固定数值函数）
    state_analyzer = MarketStateAnalyzer(
        six_points_func=get_six_points_by_price,
        three_points_func=get_three_points_by_price
    )
    df_with_states = state_analyzer.analyze(df, price_col='收盘', date_col='date')
    
    # 定义上升类型和下降类型趋势
    UP_TRENDS = ['上升趋势', '自然回升', '次级回升']
    DOWN_TRENDS = ['下降趋势', '自然回撤', '次级回撤']
    
    # === 新增：卖出条件标记 ===
    # 当自然回升→上升趋势但未突破前高时，标记下次上升趋势→自然回撤需要卖出
    sell_on_next_up_trend_to_reaction = False
    # 记录上一次上升趋势的最高点
    last_up_trend_high = None
    # 记录上一次下降趋势的最低点
    last_down_trend_low = None
    
    # 收集所有输出内容
    output_lines = []

    def log_print(*args, **kwargs):
        """同时打印到终端和收集到列表"""
        line = " ".join(str(arg) for arg in args)
        print(line, **kwargs)
        output_lines.append(line)

    # 记录最近一次买入（用于“买入后第二天转跌”的观察基准）
    last_transition_buy_idx = -999999
    last_transition_buy_price = 0.0
    def execute_transition_buy(date_str: str, close_price: float, current_total_value: float, day_idx: int, full_position: bool = False):
        """执行买入，返回动作文本（失败返回空字符串）
        
        Args:
            full_position: True表示一次性买入所有剩余仓位，False表示只买入1仓
        """
        nonlocal cash, position, position_cost, position_count, last_buy_price, invested_capital, fixed_position_value, last_transition_buy_idx, last_transition_buy_price

        if position_count >= POSITION_COUNT:
            return ""

        if position_count == 0:
            fixed_position_value = current_total_value / POSITION_COUNT
        
        # 确定要买入的仓位数量
        if full_position:
            # 一次性买入所有剩余仓位
            positions_to_buy = POSITION_COUNT - position_count
        else:
            # 只买入1仓
            positions_to_buy = 1
        
        total_shares = 0
        total_value = 0
        start_position_count = position_count
        
        for _ in range(positions_to_buy):
            position_value = fixed_position_value
            if cash < position_value * 0.99:
                break
            
            shares = position_value / close_price
            prev_position = position
            position += shares
            if prev_position <= 0:
                position_cost = close_price
            else:
                total_cost = position_cost * prev_position + close_price * shares
                position_cost = total_cost / position
            invested_capital += shares * close_price
            cash -= shares * close_price
            position_count += 1
            total_shares += shares
            total_value += shares * close_price
            
            trades.append({
                'date': date_str,
                'action': '买入',
                'position_num': position_count,
                'price': close_price,
                'shares': shares,
                'value': shares * close_price
            })
        
        if position_count > start_position_count:
            last_buy_price = close_price
            last_transition_buy_idx = day_idx
            last_transition_buy_price = close_price
            if full_position and positions_to_buy > 1:
                return f"[买入{start_position_count + 1}-{position_count}/{POSITION_COUNT}](满仓)"
            else:
                return f"[买入{position_count}/{POSITION_COUNT}]"
        return ""

    # 全局延迟买入状态：所有买点先记录切换价格，等待上涨到阈值后再买
    pending_buy_signal = None
    
    # 所有上升趋势的特殊卖出跟踪（突破前高2%内，回落THREE_POINTS卖出）
    uptrend_sell_active = False         # 是否启用上升趋势特殊卖出
    uptrend_sell_high = 0.0             # 该上升趋势的最高点
    uptrend_sell_ref_high = 0.0         # 上一轮上升趋势的高点
    
    # 所有自然回升的特殊卖出跟踪（突破前高2%内，回落THREE_POINTS卖出）
    natural_rally_sell_active = False   # 是否启用自然回升特殊卖出
    natural_rally_sell_high = 0.0       # 该自然回升的最高点
    natural_rally_sell_ref_high = 0.0   # 上一轮自然回升的高点
    
    # 自然回升中超过前低买入跟踪（下降趋势→自然回升因破前低跳过买入后，在自然回升中超过前低时买入）
    natural_rally_breakout_buy_active = False  # 是否启用自然回升突破前低买入
    natural_rally_breakout_ref_low = 0.0       # 前一轮下降趋势的低点（突破目标）
    
    # 所有下降趋势的特殊买入跟踪（跌破前低2%内，上涨THREE_POINTS买入）
    downtrend_buy_active = False        # 是否启用下降趋势特殊买入
    downtrend_buy_low = 0.0             # 该下降趋势的最低点
    downtrend_buy_ref_low = 0.0         # 上一轮下降趋势的低点
    
    # 下降趋势结束延迟买入观察状态
    downtrend_buy_pending = False       # 是否处于下降趋势结束待买入观察状态
    downtrend_buy_trigger_price = 0.0   # 下降趋势结束触发买入的价格（观察基准价）
    
    # 所有自然回撤的特殊买入跟踪（未跌破前低2%内，上涨THREE_POINTS买入）
    natural_reaction_buy_active = False  # 是否启用自然回撤特殊买入
    natural_reaction_buy_low = 0.0       # 该自然回撤的最低点
    natural_reaction_buy_ref_low = 0.0   # 上一轮自然回撤的低点
    
    # 分仓加仓控制：只有下降趋势提前结束和自然回撤结束才分仓，趋势转换一次性满仓
    enable_add_position = False  # 是否启用分仓加仓模式

    # 打印表头
    log_print(f"\n{'='*140}")
    log_print(f"股票代码: {stock_code}")
    log_print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    log_print(f"起始资金: {initial_capital:,.2f}")
    log_print(f"{'='*140}\n")

    # 动态生成表头
    header = f"{'日':<5} {'日期':<12} {'收盘':>10} {'市场状态':<12} {'段落':<6} {'关键点':>10} {'参考点':>10} {'转换信息':<50}"
    log_print(header)
    log_print("-" * 120)
    
    # 遍历每一天
    prev_state = None
    for i in range(len(df_with_states)):
        row = df_with_states.iloc[i]
        day_num = i + 1
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        close_price = row['收盘']
        market_state = row['market_state']
        is_start = row['is_segment_start']
        is_end = row['is_segment_end']
        key_point = row['key_point']
        ref_key_point = row.get('ref_key_point', None)
        notes = row['state_notes']
        allow_buy_down_to_rally = row.get('allow_buy_down_to_rally', True)
        last_down_trend_low = row.get('last_down_trend_low', None)
        
        # 段落标记（同一天可能既是结束也是开始）
        segment_marker = ""
        if is_start and is_end:
            segment_marker = "[转换]"
        elif is_start:
            segment_marker = "[开始]"
        elif is_end:
            segment_marker = "[结束]"
        
        key_point_str = f"{key_point:.2f}" if pd.notna(key_point) else ""
        ref_key_point_str = f"{ref_key_point:.2f}" if pd.notna(ref_key_point) else ""
        
        # 只在段落开始时显示转换信息（状态转换的当天）
        notes_str = notes if notes and is_start else ""
        
        # ===== 买卖逻辑 =====
        trade_action = ""

        # 计算当前总资金（现金 + 持仓市值）
        current_total_value = cash + position * close_price

        # 检查是否发生状态转换
        should_sell = False
        # 特殊场景：卖出发生时是否保留待买观察信号
        keep_pending_after_sell = False

        # 先处理待买观察：若状态变化则复位，若上涨达到阈值则买入
        if ENABLE_DELAYED_BUY_MODE and pending_buy_signal is not None:
            observe_state = pending_buy_signal['observe_state']
            base_price = pending_buy_signal['base_price']
            trigger_price = base_price * (1 + BUY_DELAY_RISE_PCT)

            if market_state != observe_state:
                # 上升类型→上升类型：继续观察不复位；其它变化（尤其上升→下降）复位
                start_idx = pending_buy_signal.get('start_idx', -999999)
                # 仅在相邻两天内的上升类型→上升类型状态变化，保留待买观察
                keep_observing = (observe_state in UP_TRENDS and market_state in UP_TRENDS and (i - start_idx) <= 1)
                if not keep_observing:
                    pending_buy_signal = None
                    if trade_action == "":
                        trade_action = f"[待买复位: 状态{observe_state}→{market_state}]"
            if pending_buy_signal is not None and close_price >= trigger_price:
                # 趋势转换买入一次性满仓（full_position=True）
                buy_action = execute_transition_buy(date_str, close_price, current_total_value, i, full_position=True)
                if buy_action:
                    transition_name = pending_buy_signal['transition']
                    trade_action = f"{buy_action}(由待买触发:{transition_name} 基准{base_price:.2f} 目标{trigger_price:.2f})"
                    enable_add_position = False  # 趋势转换买入不启用分仓加仓
                pending_buy_signal = None

        if is_start and prev_state is not None:
            # 买入条件判定
            can_buy_on_transition = True
            if prev_state == '下降趋势' and market_state == '自然回升' and not allow_buy_down_to_rally:
                can_buy_on_transition = False
                if trade_action == "":
                    trade_action = "[跳过买入: 下行破前低]"
                # 激活自然回升突破前低买入跟踪
                if ENABLE_NATURAL_RALLY_BREAKOUT_BUY:
                    natural_rally_breakout_buy_active = True
                    # 记录前一轮下降趋势的低点（last_down_trend_low）
                    if pd.notna(last_down_trend_low):
                        natural_rally_breakout_ref_low = last_down_trend_low
                    if trade_action == "[跳过买入: 下行破前低]":
                        trade_action = f"[跳过买入: 下行破前低(前低{natural_rally_breakout_ref_low:.2f})]"

            # 买入逻辑：下降类型→上升类型，或上升类型→上升类型
            can_buy_transition_pair = (
                (prev_state in DOWN_TRENDS and market_state in UP_TRENDS) or
                (prev_state in UP_TRENDS and market_state in UP_TRENDS)
            )
            if can_buy_on_transition and can_buy_transition_pair:
                # 计算相比前一天的涨幅
                prev_day_price = df_with_states.iloc[i-1]['收盘'] if i > 0 else close_price
                day_rise_pct = (close_price - prev_day_price) / prev_day_price if prev_day_price > 0 else 0
                
                # 判断是否大涨直接买入
                is_big_rise = ENABLE_DIRECT_BUY_ON_BIG_RISE and day_rise_pct >= DIRECT_BUY_RISE_THRESHOLD
                
                if ENABLE_DELAYED_BUY_MODE and not is_big_rise:
                    pending_buy_signal = {
                        'transition': f"{prev_state}→{market_state}",
                        'observe_state': market_state,
                        'base_price': close_price,
                        'start_date': date_str,
                        'start_idx': i
                    }
                    target_price = close_price * (1 + BUY_DELAY_RISE_PCT)
                    if trade_action == "":
                        trade_action = f"[待买观察:{prev_state}→{market_state} 基准{close_price:.2f} 目标{target_price:.2f}]"
                else:
                    # 非延迟模式或大涨直接买入（趋势转换买入一次性满仓）
                    buy_action = execute_transition_buy(date_str, close_price, current_total_value, i, full_position=True)
                    if buy_action:
                        enable_add_position = False  # 趋势转换买入不启用分仓加仓
                        if is_big_rise:
                            if trade_action == "":
                                trade_action = f"{buy_action}(状态切换大涨{day_rise_pct*100:.1f}%)"
                        else:
                            if trade_action == "":
                                trade_action = f"{buy_action}(状态切换当日)"

            # 所有下降趋势：初始化特殊买入跟踪（在进入下降趋势时初始化）
            if is_start and prev_state is not None and ENABLE_DOWNTREND_BUY and market_state == '下降趋势':
                # 检查是否是从其他状态转换到下降趋势
                if prev_state != '下降趋势':
                    downtrend_buy_active = True
                    downtrend_buy_low = close_price
                    # 获取上一轮下降趋势的低点作为参考
                    if pd.notna(ref_key_point):
                        downtrend_buy_ref_low = ref_key_point

            # 卖出逻辑：仅当上升类型 → 下降类型时卖出
            if prev_state in UP_TRENDS and market_state in DOWN_TRENDS:
                should_sell = True

            # 下降类型 → 下降类型 的状态转换也卖出
            if prev_state in DOWN_TRENDS and market_state in DOWN_TRENDS:
                should_sell = True

            # 仅在“买入后第二天”且价格下跌时：保留已完成买入，并以买入价为基准重启观察
            if (
                ENABLE_DELAYED_BUY_MODE and
                (i - last_transition_buy_idx) == 1 and
                last_transition_buy_price > 0 and
                close_price < last_transition_buy_price
            ):
                pending_buy_signal = {
                    'transition': '买后次日观察:价格下跌',
                    'observe_state': market_state,
                    'base_price': last_transition_buy_price,
                    'start_date': date_str,
                    'start_idx': i
                }
                keep_pending_after_sell = True
                if trade_action == "":
                    target_price = last_transition_buy_price * (1 + BUY_DELAY_RISE_PCT)
                    trade_action = f"[买后次日观察:基准{last_transition_buy_price:.2f} 目标{target_price:.2f}]"

            # 特例: 上升趋势 → 自然回撤
            # 只有当启用开关且突破前高超过阈值时才跳过卖出，否则仍然卖出
            if prev_state == '上升趋势' and market_state == '自然回撤' and ENABLE_SKIP_SELL_ON_BREAKOUT:
                current_up_trend_high = df_with_states.iloc[i - 1]['key_point'] if i > 0 else None

                broke_prev_up_high_with_threshold = (
                    last_up_trend_high is not None and
                    pd.notna(current_up_trend_high) and
                    current_up_trend_high > last_up_trend_high * (1 + BREAKOUT_THRESHOLD)
                )
                if broke_prev_up_high_with_threshold:
                    should_sell = False
                    if trade_action == "":
                        breakout_pct = ((current_up_trend_high - last_up_trend_high) / last_up_trend_high) * 100
                        trade_action = f"[跳过卖出: 上升突破前高+{breakout_pct:.1f}%]"

                # 上升趋势结束，更新“上一次上升趋势高点”供下一轮比较
                if pd.notna(current_up_trend_high):
                    last_up_trend_high = current_up_trend_high

            # 自然回升→上升趋势：记录"上一轮上升趋势高点"作为后续卖出比较基准
            if prev_state == '自然回升' and market_state == '上升趋势':
                # 优先使用状态分析器给出的参考点（上一轮上升趋势高点）
                if pd.notna(ref_key_point):
                    last_up_trend_high = ref_key_point
                # 进入新上升趋势时清空旧标记，是否卖出由后续"上升趋势→自然回撤"实时比较决定
                sell_on_next_up_trend_to_reaction = False

            # 任意状态→上升趋势：初始化上升趋势特殊卖出跟踪
            if is_start and prev_state is not None and ENABLE_UPTREND_SELL and market_state == '上升趋势':
                # 检查是否是从其他状态转换到上升趋势
                if prev_state != '上升趋势':
                    uptrend_sell_active = True
                    uptrend_sell_high = close_price
                    # 获取上一轮上升趋势的高点作为参考
                    if pd.notna(ref_key_point):
                        uptrend_sell_ref_high = ref_key_point

            # 任意状态→自然回升：初始化自然回升特殊卖出跟踪
            if is_start and prev_state is not None and ENABLE_NATURAL_RALLY_SELL and market_state == '自然回升':
                # 检查是否是从其他状态转换到自然回升
                if prev_state != '自然回升':
                    natural_rally_sell_active = True
                    natural_rally_sell_high = close_price
                    # 获取上一轮自然回升的高点作为参考
                    if pd.notna(ref_key_point):
                        natural_rally_sell_ref_high = ref_key_point

            # 上升趋势→自然回撤：初始化自然回撤特殊买入跟踪
            if is_start and prev_state is not None and ENABLE_NATURAL_REACTION_BUY and market_state == '自然回撤':
                # 检查是否是从其他状态转换到自然回撤
                if prev_state != '自然回撤':
                    natural_reaction_buy_active = True
                    natural_reaction_buy_low = close_price
                    # 获取上一轮自然回撤的低点作为参考
                    if pd.notna(ref_key_point):
                        natural_reaction_buy_ref_low = ref_key_point

            if should_sell and position > 0:
                sell_value = position * close_price
                profit = sell_value - invested_capital
                profit_pct = (profit / invested_capital) * 100 if invested_capital > 0 else 0
                cash += sell_value
                trade_action = "[卖出全部]"
                trades.append({
                    'date': date_str,
                    'action': '卖出',
                    'price': close_price,
                    'shares': position,
                    'value': sell_value,
                    'profit': profit,
                    'profit_pct': profit_pct
                })
                position = 0
                position_cost = 0
                position_count = 0
                last_buy_price = 0
                invested_capital = 0
                fixed_position_value = 0
                if not keep_pending_after_sell:
                    pending_buy_signal = None
                uptrend_sell_active = False
                uptrend_sell_high = 0.0
                uptrend_sell_ref_high = 0.0
                downtrend_buy_active = False
                downtrend_buy_low = 0.0
                downtrend_buy_ref_low = 0.0
                natural_reaction_buy_active = False
                natural_reaction_buy_low = 0.0
                natural_reaction_buy_ref_low = 0.0
                natural_rally_sell_active = False
                natural_rally_sell_high = 0.0
                natural_rally_sell_ref_high = 0.0
                enable_add_position = False  # 卖出后禁用分仓加仓

        # 所有上升趋势的特殊卖出逻辑：在上升趋势中跟踪最高点，从高点回落THREE_POINTS时卖出
        if ENABLE_UPTREND_SELL and uptrend_sell_active and market_state == '上升趋势' and position > 0:
            # 更新该上升趋势的最高点
            if close_price > uptrend_sell_high:
                uptrend_sell_high = close_price
            
            # 检查当前上升趋势的最高点是否没有突破前高（即最高点 <= 上一轮高点）
            if uptrend_sell_ref_high > 0:
                price_ratio = uptrend_sell_high / uptrend_sell_ref_high
                if price_ratio <= 1:  # 没有突破前高（最高点 <= 上一轮高点）
                    # 计算低于前高的幅度
                    below_pct = 1 - price_ratio
                    status_str = f"低于前高-{below_pct*100:.1f}%"
                    
                    # 获取当前价格对应的THREE_POINTS阈值（固定数值：元）
                    three_points_value = get_three_points_by_price(close_price)
                    if three_points_value is None:
                        three_points_value = 0.075  # 默认 fallback
                    
                    # 检查是否从该上升趋势的高点回落超过THREE_POINTS（转换为百分比）
                    drop_from_high_pct = (uptrend_sell_high - close_price) / uptrend_sell_high
                    three_points_pct = three_points_value / close_price  # 将固定数值转换为百分比
                    if drop_from_high_pct >= three_points_pct:  # 从高点回落超过THREE_POINTS才卖出
                        sell_value = position * close_price
                        profit = sell_value - invested_capital
                        profit_pct = (profit / invested_capital) * 100 if invested_capital > 0 else 0
                        cash += sell_value
                        trade_action = f"[卖出全部:上升趋势结束({status_str}后回落{drop_from_high_pct*100:.1f}%)]"
                        trades.append({
                            'date': date_str,
                            'action': '卖出',
                            'price': close_price,
                            'shares': position,
                            'value': sell_value,
                            'profit': profit,
                            'profit_pct': profit_pct
                        })
                        position = 0
                        position_cost = 0
                        position_count = 0
                        last_buy_price = 0
                        invested_capital = 0
                        fixed_position_value = 0
                        pending_buy_signal = None
                        uptrend_sell_active = False
                        uptrend_sell_high = 0.0
                        uptrend_sell_ref_high = 0.0

        # 所有自然回升的特殊卖出逻辑：在自然回升中跟踪最高点，从高点回落THREE_POINTS时卖出
        if ENABLE_NATURAL_RALLY_SELL and natural_rally_sell_active and market_state == '自然回升' and position > 0:
            # 更新该自然回升的最高点
            if close_price > natural_rally_sell_high:
                natural_rally_sell_high = close_price
            
            # 检查当前自然回升的最高点是否在上一轮高点的102%以内（包括未突破和突破2%以内）
            if natural_rally_sell_ref_high > 0:
                price_ratio = natural_rally_sell_high / natural_rally_sell_ref_high
                if price_ratio <= (1 + NATURAL_RALLY_BREAKOUT_THRESHOLD):  # 在上一轮高点的102%以内
                    # 计算相对上一轮高点的变化幅度
                    if price_ratio >= 1:
                        breakout_pct = price_ratio - 1  # 突破幅度
                        status_str = f"突破+{breakout_pct*100:.1f}%"
                    else:
                        below_pct = 1 - price_ratio  # 低于幅度
                        status_str = f"低于前高-{below_pct*100:.1f}%"
                    
                    # 获取当前价格对应的THREE_POINTS阈值（固定数值：元）
                    three_points_value = get_three_points_by_price(close_price)
                    if three_points_value is None:
                        three_points_value = 0.075  # 默认 fallback
                    
                    # 检查是否从该自然回升的高点回落超过THREE_POINTS（转换为百分比）
                    drop_from_high_pct = (natural_rally_sell_high - close_price) / natural_rally_sell_high
                    three_points_pct = three_points_value / close_price  # 将固定数值转换为百分比
                    if drop_from_high_pct >= three_points_pct:  # 从高点回落超过THREE_POINTS才卖出
                        sell_value = position * close_price
                        profit = sell_value - invested_capital
                        profit_pct = (profit / invested_capital) * 100 if invested_capital > 0 else 0
                        cash += sell_value
                        trade_action = f"[卖出全部:自然回升结束({status_str}后回落{drop_from_high_pct*100:.1f}%)]"
                        trades.append({
                            'date': date_str,
                            'action': '卖出',
                            'price': close_price,
                            'shares': position,
                            'value': sell_value,
                            'profit': profit,
                            'profit_pct': profit_pct
                        })
                        position = 0
                        position_cost = 0
                        position_count = 0
                        last_buy_price = 0
                        invested_capital = 0
                        fixed_position_value = 0
                        pending_buy_signal = None
                        natural_rally_sell_active = False
                        natural_rally_sell_high = 0.0
                        natural_rally_sell_ref_high = 0.0

        # 自然回升中超过前低买入逻辑：下降趋势→自然回升因破前低跳过买入后，在自然回升中超过前低时买入
        if ENABLE_NATURAL_RALLY_BREAKOUT_BUY and natural_rally_breakout_buy_active and market_state == '自然回升' and position == 0:
            if natural_rally_breakout_ref_low > 0:
                # 检查当前价格是否超过前低（加上阈值）
                breakout_target = natural_rally_breakout_ref_low * (1 + NATURAL_RALLY_BREAKOUT_BUY_THRESHOLD)
                if close_price >= breakout_target:
                    # 买入（启用分仓加仓模式）
                    buy_action = execute_transition_buy(date_str, close_price, current_total_value, i)
                    if buy_action:
                        above_pct = (close_price - natural_rally_breakout_ref_low) / natural_rally_breakout_ref_low
                        trade_action = f"{buy_action}(自然回升突破前低:{natural_rally_breakout_ref_low:.2f}→{close_price:.2f} +{above_pct*100:.1f}%)"
                        natural_rally_breakout_buy_active = False
                        natural_rally_breakout_ref_low = 0.0
                        enable_add_position = True  # 启用分仓加仓

        # 所有下降趋势的特殊买入逻辑：在下降趋势中跟踪最低点，从低点上涨THREE_POINTS时进入待观察状态
        if ENABLE_DOWNTREND_BUY and downtrend_buy_active and market_state == '下降趋势' and position == 0:
            # 更新该下降趋势的最低点
            if close_price < downtrend_buy_low:
                downtrend_buy_low = close_price
            
            # 检查当前下降趋势的最低点是否没有跌破上一轮低点（即当前低点 >= 前低）
            if downtrend_buy_ref_low > 0:
                if downtrend_buy_low >= downtrend_buy_ref_low:  # 必须没有跌破前低
                    # 计算高于前低的幅度
                    above_pct = (downtrend_buy_low - downtrend_buy_ref_low) / downtrend_buy_ref_low
                    status_str = f"未破前低+{above_pct*100:.1f}%"
                    
                    # 获取当前价格对应的THREE_POINTS阈值（固定数值：元）
                    three_points_value = get_three_points_by_price(close_price)
                    if three_points_value is None:
                        three_points_value = 0.075  # 默认 fallback
                    
                    # 检查是否从该下降趋势的低点上涨超过THREE_POINTS（转换为百分比）
                    rise_from_low_pct = (close_price - downtrend_buy_low) / downtrend_buy_low
                    three_points_pct = three_points_value / close_price  # 将固定数值转换为百分比
                    if rise_from_low_pct >= three_points_pct:  # 从低点上涨超过THREE_POINTS才触发
                        if ENABLE_DOWNTREND_BUY_DELAY:
                            # 进入延迟观察状态，等待价格超过触发点
                            if not downtrend_buy_pending:
                                downtrend_buy_pending = True
                                downtrend_buy_trigger_price = close_price
                                target_price = close_price * (1 + DOWNTREND_BUY_DELAY_PCT)
                                if trade_action == "":
                                    trade_action = f"[待买观察:下降趋势结束 基准{close_price:.2f} 目标{target_price:.2f}]"
                        else:
                            # 不延迟，直接买入（启用分仓加仓模式）
                            buy_action = execute_transition_buy(date_str, close_price, current_total_value, i)
                            if buy_action:
                                trade_action = f"{buy_action}(下降趋势结束:{status_str}后上涨{rise_from_low_pct*100:.1f}%)"
                                downtrend_buy_active = False
                                downtrend_buy_low = 0.0
                                downtrend_buy_ref_low = 0.0
                                enable_add_position = True  # 启用分仓加仓
        
        # 处理下降趋势结束延迟买入观察：价格超过触发点后买入
        if ENABLE_DOWNTREND_BUY_DELAY and downtrend_buy_pending and position == 0:
            target_price = downtrend_buy_trigger_price * (1 + DOWNTREND_BUY_DELAY_PCT)
            if close_price >= target_price:
                # 价格超过目标，执行买入（启用分仓加仓模式）
                buy_action = execute_transition_buy(date_str, close_price, current_total_value, i)
                if buy_action:
                    trade_action = f"{buy_action}(由待买触发:下降趋势结束 基准{downtrend_buy_trigger_price:.2f} 目标{target_price:.2f})"
                    downtrend_buy_pending = False
                    downtrend_buy_trigger_price = 0.0
                    downtrend_buy_active = False
                    downtrend_buy_low = 0.0
                    downtrend_buy_ref_low = 0.0
                    enable_add_position = True  # 启用分仓加仓
            elif market_state != '下降趋势':
                # 状态变化，复位观察
                downtrend_buy_pending = False
                downtrend_buy_trigger_price = 0.0
                if trade_action == "":
                    trade_action = "[待买复位:下降趋势结束观察]"
        
        # 离开下降趋势时，清空待买入观察状态
        if market_state != '下降趋势':
            downtrend_buy_pending = False
            downtrend_buy_trigger_price = 0.0

        # 所有自然回撤的特殊买入逻辑：在自然回撤中跟踪最低点，从低点上涨THREE_POINTS时买入
        if ENABLE_NATURAL_REACTION_BUY and natural_reaction_buy_active and market_state == '自然回撤' and position == 0:
            # 更新该自然回撤的最低点
            if close_price < natural_reaction_buy_low:
                natural_reaction_buy_low = close_price

            # 检查当前自然回撤的最低点是否没有跌破上一轮低点（即当前低点 >= 前低 * 0.98）
            if natural_reaction_buy_ref_low > 0:
                price_ratio = natural_reaction_buy_low / natural_reaction_buy_ref_low
                if price_ratio >= (1 - NATURAL_REACTION_BREAKOUT_THRESHOLD):  # 在上一轮低点的98%以上
                    # 计算相对上一轮低点的变化幅度
                    if price_ratio >= 1:
                        above_pct = price_ratio - 1  # 高于幅度
                        status_str = f"高于前低+{above_pct*100:.1f}%"
                    else:
                        below_pct = 1 - price_ratio  # 低于幅度
                        status_str = f"低于前低-{below_pct*100:.1f}%"

                    # 获取当前价格对应的THREE_POINTS阈值（固定数值：元）
                    three_points_value = get_three_points_by_price(close_price)
                    if three_points_value is None:
                        three_points_value = 0.075  # 默认 fallback

                    # 检查是否从该自然回撤的低点上涨超过THREE_POINTS（转换为百分比）
                    rise_from_low_pct = (close_price - natural_reaction_buy_low) / natural_reaction_buy_low
                    three_points_pct = three_points_value / close_price  # 将固定数值转换为百分比
                    if rise_from_low_pct >= three_points_pct:  # 从低点上涨超过THREE_POINTS才买入
                        buy_action = execute_transition_buy(date_str, close_price, current_total_value, i)
                        if buy_action:
                            trade_action = f"{buy_action}(自然回撤结束:{status_str}后上涨{rise_from_low_pct*100:.1f}%)"
                            natural_reaction_buy_active = False
                            natural_reaction_buy_low = 0.0
                            natural_reaction_buy_ref_low = 0.0
                            enable_add_position = True  # 启用分仓加仓
        
        # 离开自然回撤时，清空自然回撤买入跟踪
        if market_state != '自然回撤':
            natural_reaction_buy_active = False
            natural_reaction_buy_low = 0.0
            natural_reaction_buy_ref_low = 0.0

        # 离开自然回升时，清空自然回升突破前低买入跟踪
        if market_state != '自然回升':
            natural_rally_breakout_buy_active = False
            natural_rally_breakout_ref_low = 0.0

        # 加仓逻辑：只有在启用分仓加仓模式时，价格每上涨ADD_POSITION_THRESHOLD买入下一仓
        # 在任意趋势（上升类型和下降类型）中都执行加仓
        if enable_add_position and position > 0 and position_count < POSITION_COUNT:
            if close_price >= last_buy_price * (1 + ADD_POSITION_THRESHOLD):
                buy_action = execute_transition_buy(date_str, close_price, current_total_value, i)
                if buy_action:
                    trade_action = f"{buy_action}(加仓:上涨{(close_price/last_buy_price-1)*100:.1f}%)"

        # 更新前一天的状态
        if is_start:
            prev_state = market_state
        
        # 构建输出行
        output_str = f"{day_num:<5} {date_str:<12} {close_price:>10.2f} {market_state:<12} {segment_marker:<6} {key_point_str:>10} {ref_key_point_str:>10} {notes_str:<50}"
        if trade_action:
            output_str += f" {trade_action}"
        log_print(output_str)
    
    log_print(f"{'='*140}")
    
    # 打印状态段落摘要
    log_print("\n【市场状态段落摘要】")
    segments_df = state_analyzer.get_segments_summary()
    for i, seg in segments_df.iterrows():
        duration = seg['持续天数']
        end_date = seg['结束日期'] if seg['结束日期'] else '现在'
        key_point_type = "最高点" if seg['状态'] in ['上升趋势', '自然回升', '次级回升'] else "最低点"
        end_price_str = f"{seg['结束价格']:.2f}" if pd.notna(seg['结束价格']) else '...'
        
        # 计算段落内的价格变化
        if pd.notna(seg['结束价格']):
            seg_change = seg['结束价格'] - seg['开始价格']
            seg_change_pct = (seg_change / seg['开始价格']) * 100
            change_str = f"{seg_change:+.2f}({seg_change_pct:+.2f}%)"
        else:
            change_str = "进行中"
        
        log_print(f"  [{i}] {seg['状态']}: {seg['开始日期']}~{end_date} | "
                  f"价格:{seg['开始价格']:.2f}→{end_price_str} {change_str} | "
                  f"{key_point_type}:{seg['关键点']:.2f} | {duration}天")
    
    # 计算最终收益（包括持仓市值）
    final_price = df_with_states.iloc[-1]['收盘']
    position_value = position * final_price
    final_value = cash + position_value
    final_profit = final_value - initial_capital
    total_return = (final_profit / initial_capital) * 100
    
    log_print(f"\n{'='*140}")
    log_print(f"最终资金: {final_value:,.2f} (现金: {cash:,.2f} + 持仓市值: {position_value:,.2f})")
    log_print(f"总盈亏: {final_profit:,.2f} ({total_return:+.2f}%)")
    log_print(f"交易次数: {len(trades)}")
    
    # 打印交易记录
    if trades:
        log_print(f"\n【交易记录】")
        for trade in trades:
            if trade['action'] == '买入':
                log_print(f"  买入: {trade['date']} | 仓位: {trade['position_num']}/{POSITION_COUNT} | 价格: {trade['price']:.2f} | 数量: {trade['shares']:.0f} | 金额: {trade['value']:,.2f}")
            else:
                log_print(f"  卖出: {trade['date']} | 价格: {trade['price']:.2f} | 数量: {trade['shares']:.0f} | 金额: {trade['value']:,.2f} | 盈亏: {trade['profit']:+.2f} ({trade['profit_pct']:+.2f}%)")
    
    log_print(f"{'='*140}")
    
    # 保存到文件
    output_file = get_output_file_path()
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
        print(f"\n[文件已保存至: {output_file}]")
    except Exception as e:
        print(f"\n[警告: 无法保存文件 - {e}]")
    
    return total_return, {}, {'trades': trades, 'final_value': final_value, 'holding_info': None, 'state_segments': segments_df.to_dict('records')}


if __name__ == "__main__":
    run_backtest()














