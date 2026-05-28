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

from generate_chart_with_states import calculate_trend_break_markers

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

# 仓位管理配置

# 单仓模式：不做加仓

STOP_LOSS_PCT = 0.05  # 止损比例，默认5%

# MACD卖出阈值

MACD_SELL_THRESHOLD = 0.03  # MACD从大变小的幅度阈值

# MACD卖出阈值 - 上升趋势

MACD_SELL_THRESHOLD_UPTREND = -0.15

# 自然回升MACD高值保护阈值 - 当MACD大于此值时，即使变低也不卖出

NATURAL_RALLY_MACD_PROTECT_THRESHOLD = 1.0  # 可配置，默认1.0



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

def build_trend_break_events(df: pd.DataFrame) -> dict:

    """预计算每一天的趋势线事件。

    返回格式：

    {"

      '2024-01-01': {

        'up_break': True/False,

        'down_break': True/False,

        'up_marker': '...',

        'down_marker': '...'

      }

    }

    """

    if df is None or df.empty:

        return {}

    date_values = df['date']

    if hasattr(date_values.iloc[0], 'strftime'):

        date_list = [d.strftime('%Y-%m-%d') for d in date_values]

    else:

        date_list = [str(d)[:10] for d in date_values]

    price_list = [float(x) for x in df['收盘'].tolist()]

    market_states = [

        str(state).split('|')[0] if pd.notna(state) else ''

        for state in df['market_state'].tolist()

    ]

    markers_by_date = calculate_trend_break_markers(date_list, price_list, market_states)

    trend_events = {}

    for date, markers in markers_by_date.items():

        day_event = {

            'up_break': False,

            'down_break': False,

            'up_markers': [],

            'down_markers': [],

            'up_marker': '',

            'down_marker': '',

        }

        for marker in markers:

            if not isinstance(marker, str):

                continue

            if marker.startswith('上升趋势线跌破@'):

                day_event['up_break'] = True

                day_event['up_markers'].append(marker)

                if not day_event['up_marker']:

                    day_event['up_marker'] = marker

            elif marker.startswith('下降趋势线上破@'):

                day_event['down_break'] = True

                day_event['down_markers'].append(marker)

                if not day_event['down_marker']:

                    day_event['down_marker'] = marker

        trend_events[date] = day_event

    return trend_events

def _extract_anchor_dates(marker: str):

    """Parse anchor1/2 dates in a trend marker."""

    if not isinstance(marker, str):

        return None, None

    anchor1_date = None

    anchor2_date = None

    for part in marker.split("|"):

        if part.startswith("锚点1:"):

            try:

                date_part = part.split("锚点1:", 1)[1].split("@", 1)[0].strip()

                anchor1_date = pd.to_datetime(date_part, errors="coerce").date()

            except Exception:

                anchor1_date = None

        elif part.startswith("锚点2:"):

            try:

                date_part = part.split("锚点2:", 1)[1].split("@", 1)[0].strip()

                anchor2_date = pd.to_datetime(date_part, errors="coerce").date()

            except Exception:

                anchor2_date = None

    return anchor1_date, anchor2_date

def _select_trend_break_marker(markers):

    # Pick a trend-break marker for current state.

    if not markers:

        return ''

    parsed = []

    for marker in markers:

        if not isinstance(marker, str):

            continue

        anchor1_date, anchor2_date = _extract_anchor_dates(marker)

        if anchor1_date is None:

            continue

        parsed.append((pd.to_datetime(anchor1_date), pd.to_datetime(anchor2_date) if anchor2_date else None, marker))

    if not parsed:

        return ''

    return str(max(parsed, key=lambda x: (x[0], x[1] if x[1] is not None else pd.Timestamp.min))[2])

def _parse_trend_break_marker(marker: str):

    """解析趋势线标记，返回锚点与斜率信息。"""

    if not isinstance(marker, str):

        return None

    if "@" not in marker or "锚点1:" not in marker or "锚点2:" not in marker:

        return None

    if marker.startswith("上升趋势线跌破@"):

        trend_type = "up"

        body = marker[len("上升趋势线跌破@"):]

    elif marker.startswith("下降趋势线上破@"):

        trend_type = "down"

        body = marker[len("下降趋势线上破@"):]

    else:

        return None

    price_parts = body.split("|", 1)

    line_part = price_parts[0]

    anchor_parts = body.split("|")

    if "/" not in line_part:

        return None

    try:

        break_price_str, line_price_part = line_part.split("/", 1)

        break_price = float(break_price_str)

        if not line_price_part.startswith("线"):

            return None

        line_price = float(line_price_part[1:])

    except Exception:

        return None

    anchor1_date = anchor1_price = None

    anchor2_date = anchor2_price = None

    angle = None

    slope = None

    for part in anchor_parts[1:]:

        part = part.strip()

        if part.startswith("锚点1:"):

            try:

                value = part.split("锚点1:", 1)[1]

                date_part, price_part = value.split("@", 1)

                anchor1_date = pd.to_datetime(date_part.strip(), errors="coerce").date()

                anchor1_price = float(price_part)

            except Exception:

                anchor1_date = None

                anchor1_price = None

        elif part.startswith("锚点2:"):

            try:

                value = part.split("锚点2:", 1)[1]

                date_part, price_part = value.split("@", 1)

                anchor2_date = pd.to_datetime(date_part.strip(), errors="coerce").date()

                anchor2_price = float(price_part)

            except Exception:

                anchor2_date = None

                anchor2_price = None

        elif part.startswith("角度:"):

            try:

                angle = float(part.split("角度:", 1)[1].replace("°", "").strip())

            except Exception:

                angle = None

        elif part.startswith("斜率:"):

            try:

                slope = float(part.split("斜率:", 1)[1].strip())

            except Exception:

                slope = None

    if anchor1_date is None or anchor2_date is None or anchor1_price is None or anchor2_price is None:

        return None

    return {

        "trend_type": trend_type,

        "break_price": break_price,

        "line_price": line_price,

        "anchor1_date": anchor1_date,

        "anchor1_price": anchor1_price,

        "anchor2_date": anchor2_date,

        "anchor2_price": anchor2_price,

        "angle": angle,

        "slope": slope,

        "raw": marker

    }

def _format_marker_slope(marker_info):

    """从 marker 信息中提取可显示的斜率文本。"""

    if not marker_info:

        return ""

    if marker_info.get("angle") is not None:

        return f" 斜率:{marker_info['angle']:.2f}°"

    if marker_info.get("slope") is not None:

        return f"斜率:{marker_info['slope']:.4f}"

    return ""

def _calc_line_price_at(marker_info, current_index, date_to_index):

    """根据锚点坐标计算给定交易日对应的趋势线价格。"""

    if not marker_info:

        return None

    idx1 = date_to_index.get(marker_info.get("anchor1_date"))

    idx2 = date_to_index.get(marker_info.get("anchor2_date"))

    if idx1 is None or idx2 is None:

        return None

    if idx1 == idx2:

        return marker_info["anchor1_price"]

    p1 = marker_info["anchor1_price"]

    p2 = marker_info["anchor2_price"]

    return p1 + (p2 - p1) * (current_index - idx1) / (idx2 - idx1)

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

    trend_events = build_trend_break_events(df)

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

    log_print(f"{'='*165}")

    # 初始化交易状态

    position = 0  # 持仓数量

    cash = 100000  # 初始资金

    trades = []  # 交易记录

    # 仓位状态

    first_buy_price = 0  # 买入参考价格

    position_buy_date = None  # 当前持仓对应的买入日期

    # 交易触发依赖趋势线事件

    # 标记今天是否刚买入/卖出

    just_bought_today = False

    just_sold_today = False

    # 日期到序号映射，便于按锚点计算趋势线价格

    date_to_index = {}

    for idx, dt in enumerate(df['date']):

        date_to_index[pd.to_datetime(dt).date()] = idx

    header = f"{'日':<5} {'日期':<10} {'收盘':>8} {'ATR'+str(ATR_PERIOD):>8} {'波动率':>8} {'DIF':>8} {'DEA':>8} {'MACD':>8} {'市场状态':>10} {'持仓':>6} {'操作':>20}"

    log_print(header)

    log_print("-" * 130)

    # 遍历每一天进行交易逻辑

    for i in range(len(df)):

        row = df.iloc[i]

        day_num = i + 1

        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]

        trade_day = row['date']

        if hasattr(trade_day, 'to_pydatetime'):

            trade_day = trade_day.to_pydatetime().date()

        else:

            trade_day = pd.to_datetime(trade_day, errors='coerce').date()

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

        trend_event = trend_events.get(date_str, {})

        up_break_markers = trend_event.get('up_markers', [])

        down_break_markers = trend_event.get('down_markers', [])

        up_trend_break_marker = _select_trend_break_marker(
            up_break_markers
        )
        down_trend_break_marker = _select_trend_break_marker(
            down_break_markers
        )
        up_trend_break = bool(up_trend_break_marker)

        down_trend_break = bool(down_trend_break_marker)

        up_trend_break_info = _parse_trend_break_marker(up_trend_break_marker) if up_trend_break else None

        down_trend_break_info = _parse_trend_break_marker(down_trend_break_marker) if down_trend_break else None

        action = ""  # 操作标记

        # ==========================================

        # 交易逻辑：仅基于趋势线事件触发（不依赖阶段）

        # 1. 上升趋势线跌破：卖出（优先）

        if position > 0 and up_trend_break and not just_bought_today:

            sell_price = close_price

            cash += position * sell_price

            profit = (sell_price - first_buy_price) * position if first_buy_price > 0 else 0

            profit_pct = (sell_price - first_buy_price) / first_buy_price * 100 if first_buy_price > 0 else 0

            action = f"上升趋势线跌破卖出@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"

            if up_trend_break_marker:

                action += f" 趋势破位:{up_trend_break_marker}"

                action += _format_marker_slope(up_trend_break_info)

            trades.append({

                'day': day_num,

                'date': date_str,

                'action': '上升趋势线跌破卖出',

                'price': sell_price,

                'quantity': position,

                'profit': profit,

                'profit_pct': profit_pct,

                'angle': up_trend_break_info.get("angle") if up_trend_break_info else None,

                'slope': up_trend_break_info.get("slope") if up_trend_break_info else None

            })

            position = 0

            first_buy_price = 0

            position_buy_date = None
            just_sold_today = True
            
        # 2. 下降趋势线上破：买入

        if position == 0 and down_trend_break and not just_sold_today and not just_bought_today:

            buy_price = close_price

            buy_budget = cash

            buy_quantity = int(buy_budget / buy_price / 100) * 100

            actual_buy_amount = buy_quantity * buy_price

            if buy_quantity > 0:

                position = buy_quantity

                cash -= actual_buy_amount

                first_buy_price = buy_price

                position_buy_date = trade_day

                just_bought_today = True

                action = f"下降趋势线上破买入@{buy_price:.2f} x{position}股"

                if down_trend_break_marker:

                    action += f" 趋势破位:{down_trend_break_marker}"

                    action += _format_marker_slope(down_trend_break_info)

                trades.append({

                    'day': day_num,

                    'date': date_str,

                    'action': '下降趋势线上破买入',

                    'price': buy_price,

                    'quantity': position,

                    'angle': down_trend_break_info.get("angle") if down_trend_break_info else None,

                    'slope': down_trend_break_info.get("slope") if down_trend_break_info else None

                })

        # 4. 止损逻辑：跌幅超过5%就清仓

        if position > 0 and not just_bought_today and not just_sold_today:

            stop_loss_hit = first_buy_price > 0 and close_price <= first_buy_price * (1 - STOP_LOSS_PCT)

            if stop_loss_hit:

                sell_price = close_price

                profit = (sell_price - first_buy_price) * position

                profit_pct = (sell_price - first_buy_price) / first_buy_price * 100 if first_buy_price > 0 else 0

                action = f"止损卖出@{sell_price:.2f} 盈亏:{profit:+.0f}({profit_pct:+.2f}%)"

                trades.append({

                    'day': day_num,

                    'date': date_str,

                    'action': '止损卖出',

                    'price': sell_price,

                    'quantity': position,

                    'profit': profit,

                    'profit_pct': profit_pct

                })

                cash += position * sell_price

                position = 0

                first_buy_price = 0

                position_buy_date = None
            just_sold_today = True
            
        # ==========================================

        # 格式化输出

        # ==========================================

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

        line += f" {action:>30}"

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

            suffix = ""

            if trade.get("angle") is not None:

                suffix = f" 斜率:{trade['angle']:.2f}°"

            elif trade.get("slope") is not None:

                suffix = f" 斜率:{trade['slope']:.4f}"

            if trade['action'] == '下降趋势线上破买入':

                log_print(f"  买入: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股{suffix}")

            elif trade['action'] == '上升趋势线跌破卖出':

                log_print(f"  上升趋势线跌破卖出: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)" + suffix)

            elif trade['action'] == '止损卖出':

                log_print(f"  止损卖出: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股 盈亏:{trade['profit']:+.0f}({trade['profit_pct']:+.2f}%)")

            else:

                log_print(f"  {trade['action']}: {trade['date']} @ {trade['price']:.2f} x {trade['quantity']}股")

    log_print(f"{'='*130}")

    # 写入文件

    output_file = get_output_file_path()

    try:

        with open(output_file, 'w', encoding='utf-8') as f:

            f.write('\n'.join(output_lines))

        try:

            from generate_chart_with_states import annotate_output_file_with_trend_breaks

            annotate_output_file_with_trend_breaks(output_file)

        except Exception as marker_error:

            print(f"\n[警告: 趋势破位标记写入失败 - {marker_error}]")

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

            # 调用 generate_chart_with_states.py 生成带市场状态的图表

            script_dir = os.path.dirname(os.path.abspath(__file__))

            chart_script = os.path.join(script_dir, 'generate_chart_with_states.py')

            if os.path.exists(chart_script):

                subprocess.run([sys.executable, chart_script], check=True)

                # 打开生成的HTML文件

                html_file = os.path.join(script_dir, 'stock_chart_with_states.html')

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
