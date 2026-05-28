#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import json
import os
import sys
import math

# 市场状态颜色配置
STATE_COLORS = {
    '上升趋势': '#ff6b6b',      # 红色
    '自然回撤': '#4ecdc4',      # 青色
    '下降趋势': '#45b7d1',      # 蓝色
    '自然回升': '#96ceb4',      # 绿色
    '次级回撤': '#dfe6e9',      # 浅灰
    '次级回升': '#fdcb6e',      # 黄色
}

# 上升趋势低点趋势线配置
TREND_LINE_CONFIG = {
    'min_decline_pct': 5.0,          # 前高到低点至少回撤幅度
    'min_rebound_pct': 5.0,          # 低点后至少反弹幅度，用于延迟确认低点
    'pivot_window': 3,               # 低点后最少等待交易日数，避免一天噪声
    'pivot_high_lookback': 60,       # 找前高时最多向前看的交易日数
    'pivot_rebound_lookahead': 60,   # 找低点后反弹时最多向后看的交易日数
    'min_low_gap_days': 1,           # 两个低点太近时只保留更低/更强的一个（已放宽为不限制）
    'min_anchor_gap_days': 1,        # 两个锚点间隔最小交易日数（设为1，取消强约束）
    'min_low_rise_pct': 0.0,         # 第二个低点允许与第一个持平/略高，增加可触发线的覆盖度
    'min_slope_angle': 30,           # 实时趋势线最小斜率角度（上升线更易触发）
    'max_slope_angle': 80,           # 实时趋势线最大斜率角度
    'target_slope_angle': 50,        # 排序时偏好的角度
    'price_scale': 2.0,              # 价格缩放因子，用于调整角度敏感度
    'break_tolerance_pct': 0.6,      # 收盘价偏离趋势线超过该比例时，记入破位计数
    'break_jump_pct': 5.0,           # 单日突破线超过该比例直接判定为破位（可按需设置）
    'break_confirm_days': 2,         # 连续2个交易日偏离才确认趋势线失效
    'touch_tolerance_pct': 1.2,      # 价格贴近趋势线该比例以内，标为交汇/支撑点
    'min_touch_gap_days': 5,         # 交汇点标注之间最小间隔，减少标签重叠
    'max_touch_marks': 8,            # 单条趋势线最多显示的价格标注
    'max_trend_lines': 50,           # 实时确认线可以持续增加，这里只做防爆量上限
    'confirm_interval_days': 5,      # 每隔几个交易日巡检一次补充确认
    'show_broken_lines': True,       # 已破位趋势线只画到破位日，并标注破位价格
    'min_active_line_days': 0,       # 刚确认就破位也显示，不用存活天数过滤
    'allow_negative_slope': False,   # 这里只画上升趋势线
}

# 下降趋势高点趋势线配置：与上升趋势线相反，用两个逐步降低的高点画压力线
DOWN_TREND_LINE_CONFIG = dict(TREND_LINE_CONFIG)
DOWN_TREND_LINE_CONFIG.update({
    'min_rise_pct': 4.0,              # 前低到高点至少反弹幅度
    'min_pullback_pct': 4.0,          # 高点后至少回落幅度，用于延迟确认高点
    'min_high_gap_days': 1,           # 两个高点太近时只保留更高/更强的一个（已放宽为不限制）
    'min_anchor_gap_days': 1,        # 两个锚点间隔最小交易日数（设为1，取消强约束）
    'min_high_decline_pct': 0.1,      # 第二个高点必须比第一个高点降低
    'max_intermediate_high_distance_pct': 10.0,  # 中间高点离压力线太远时，不认为是有效趋势线
    'break_confirm_days': 3,          # 下降压力线上破需连续3天才确认
    'break_jump_pct': 5.0,          # 下跌压力线单日上破超过该比例则直接认定上破（可按需设置）
    'min_active_line_days': 0,        # 刚确认就上破也显示，不用存活天数过滤
    'allow_negative_slope': True,
    'min_slope_angle': 45,            # 下跌趋势线目标角度下限（负值）
    'max_slope_angle': 60,            # 下跌趋势线目标角度上限（负值）
    'target_slope_angle': 52,         # 下跌趋势线优先角度
    'price_scale': 2.0,               # 与上升线对齐计算口径，便于同样方法下形成更多可用压力线
})

TREND_MARKER_RE = re.compile(r'\s+趋势破位:[^\s]+')


def _strip_trend_markers(line):
    return TREND_MARKER_RE.sub('', line).rstrip()


def _parse_output_lines(raw_lines):
    dates = []
    prices = []
    market_states = []  # 每天的市场状态
    buy_points = []
    sell_points = []

    for raw_line in raw_lines:
        line = _strip_trend_markers(raw_line.strip())
        if not line or line.startswith('=') or line.startswith('日') or line.startswith('-'):
            continue

        parts = line.split()
        if len(parts) < 10:
            continue

        try:
            date_str = parts[1]
            close_price = float(parts[2])

            # 提取市场状态（在第9列，索引8）
            # 市场状态可能包含额外的信息，如 "上升趋势|从自然回升→上升趋势"
            state_str = parts[8] if len(parts) > 8 else ""
            # 提取基本状态（去掉 | 后面的内容）
            base_state = state_str.split('|')[0] if '|' in state_str else state_str

            dates.append(date_str)
            prices.append(close_price)
            market_states.append(base_state)

            buy_match = re.search(
                r'((?:上升确认买入|上升重启买入|上升趋势线站上买入|下降趋势线上破买入|买入|加仓))@([\d.]+)(?:\s*x(\d+)股)?',
                line
            )
            if buy_match:
                buy_points.append({
                    'date': date_str,
                    'price': float(buy_match.group(2)),
                    'type': buy_match.group(1),
                    'quantity': int(buy_match.group(3)) if buy_match.group(3) else None
                })
            else:
                old_buy_match = re.search(r'买入@([\d.]+)\(([^)]+)\)', line)
                if old_buy_match:
                    buy_points.append({
                        'date': date_str,
                        'price': float(old_buy_match.group(1)),
                        'type': old_buy_match.group(2),
                        'quantity': None
                    })

            sell_match = re.search(
                r'((?:上升结束清仓|清仓|止损卖出|上升趋势线跌破卖出|卖出))@([\d.]+)\s+盈亏:([+-]?\d+(?:\.\d+)?)\(([+-]?[\d.]+)%\)',
                line
            )
            if sell_match:
                sell_points.append({
                    'date': date_str,
                    'price': float(sell_match.group(2)),
                    'type': sell_match.group(1),
                    'profit': sell_match.group(3),
                    'profit_pct': sell_match.group(4)
                })
        except:
            continue

    return dates, prices, market_states, buy_points, sell_points


def parse_output_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return _parse_output_lines(f.readlines())

def generate_market_state_areas(dates, market_states):
    """生成市场状态的分段区域数据"""
    areas = []
    if not market_states or len(market_states) == 0:
        return areas
    
    current_state = market_states[0]
    start_idx = 0
    
    for i in range(1, len(market_states)):
        if market_states[i] != current_state:
            # 状态变化，记录前一个状态区域
            color = STATE_COLORS.get(current_state, '#cccccc')
            areas.append({
                'state': current_state,
                'start_date': dates[start_idx],
                'end_date': dates[i-1],
                'color': color
            })
            current_state = market_states[i]
            start_idx = i
    
    # 添加最后一个区域
    color = STATE_COLORS.get(current_state, '#cccccc')
    areas.append({
        'state': current_state,
        'start_date': dates[start_idx],
        'end_date': dates[-1],
        'color': color
    })
    
    return areas


def _state_allowed(state, allowed_states):
    return state in allowed_states


def _line_price(start_idx, start_price, slope, idx):
    return start_price + slope * (idx - start_idx)


def _is_local_low(values, idx, left_idx, right_idx):
    price = values[idx]
    window = values[left_idx:right_idx + 1]
    if price != min(window):
        return False
    # 同一个低点平台只取第一次出现的位置，避免重复画点。
    for k in range(left_idx, idx):
        if values[k] <= price:
            return False
    return True


def _merge_nearby_lows(lows, min_gap_days):
    if not lows:
        return []
    merged = []
    for low in sorted(lows, key=lambda x: x['index']):
        if not merged or low['index'] - merged[-1]['index'] >= min_gap_days:
            merged.append(low)
            continue

        prev = merged[-1]
        prev_strength = prev['decline_pct'] + prev['rebound_pct']
        curr_strength = low['decline_pct'] + low['rebound_pct']
        if low['price'] < prev['price'] or (low['price'] == prev['price'] and curr_strength > prev_strength):
            merged[-1] = low
    return merged


def find_uptrend_lows(dates, prices, market_states, config):
    """
    实时确认价格结构低点，不按市场状态分段。

    只按时间向前推进，不看确认日之后的数据：
    1. 当前价格相对已出现的前高回撤达到阈值，开始记录候选低点；
    2. 候选低点如果继续创新低，就实时更新；
    3. 只有后面价格从候选低点反弹达到阈值时，才在当天确认该低点；
    4. 返回的低点包含 confirmed_index/confirmed_date，画线只能从确认日之后使用。
    """
    lows = []
    if not dates or len(prices) < 3:
        return lows

    min_decline_pct = config['min_decline_pct']
    min_rebound_pct = config.get('min_rebound_pct', 5.0)
    min_confirm_days = max(1, int(config.get('pivot_window', 3)))
    high_lookback = max(min_confirm_days, int(config.get('pivot_high_lookback', 60)))
    min_low_gap_days = max(1, int(config.get('min_low_gap_days', 8)))

    candidate = None
    last_confirmed_low_index = None

    for idx, price in enumerate(prices):
        state = market_states[idx] if idx < len(market_states) else ''
        lookback_start = max(0, idx - high_lookback)
        prev_high_idx = lookback_start
        prev_high_price = prices[lookback_start]
        for scan_idx in range(lookback_start, idx + 1):
            if prices[scan_idx] >= prev_high_price:
                prev_high_price = prices[scan_idx]
                prev_high_idx = scan_idx

        if prev_high_price > price:
            decline_pct = (prev_high_price - price) / prev_high_price * 100
            if decline_pct >= min_decline_pct:
                if candidate is None or price < candidate['price']:
                    candidate = {
                        'index': idx,
                        'date': dates[idx],
                        'price': price,
                        'decline_pct': decline_pct,
                        'rebound_pct': 0.0,
                        'state': state,
                        'prev_high_index': prev_high_idx,
                        'prev_high_date': dates[prev_high_idx],
                        'prev_high_price': prev_high_price,
                    }

        if candidate is None:
            continue

        if price < candidate['price']:
            decline_pct = (prev_high_price - price) / prev_high_price * 100 if prev_high_price > price else candidate['decline_pct']
            candidate.update({
                'index': idx,
                'date': dates[idx],
                'price': price,
                'decline_pct': decline_pct,
                'rebound_pct': 0.0,
                'state': state,
                'prev_high_index': prev_high_idx,
                'prev_high_date': dates[prev_high_idx],
                'prev_high_price': prev_high_price,
            })
            continue

        rebound_pct = (price - candidate['price']) / candidate['price'] * 100
        enough_delay = idx - candidate['index'] >= min_confirm_days
        enough_gap = (
            last_confirmed_low_index is None
            or candidate['index'] - last_confirmed_low_index >= min_low_gap_days
        )
        if rebound_pct >= min_rebound_pct and enough_delay:
            if enough_gap:
                confirmed_low = dict(candidate)
                confirmed_low.update({
                    'rebound_pct': rebound_pct,
                    'confirmed_index': idx,
                    'confirmed_date': dates[idx],
                    'confirmed_price': price,
                    'confirm_lag_days': idx - candidate['index'],
                })
                lows.append(confirmed_low)
                last_confirmed_low_index = candidate['index']
            candidate = None

    return lows


def _ensure_initial_up_low_anchor(dates, prices, market_states, lows):
    """
    为了让趋势线在数据序列起点也能尽快成形，用第一个价格点补齐“起始锚点”。
    起始点仅用于连接后续已确认低点，不会替代原有真实低点。
    """
    if not dates or not prices:
        return lows

    if lows and lows[0].get('confirmed_index', lows[0]['index']) == 0:
        return lows

    anchor = {
        'index': 0,
        'date': dates[0],
        'price': round(prices[0], 2),
        'decline_pct': 0.0,
        'rebound_pct': 0.0,
        'state': market_states[0] if market_states else '',
        'prev_high_index': 0,
        'prev_high_date': dates[0],
        'prev_high_price': round(prices[0], 2),
        'confirmed_index': 0,
        'confirmed_date': dates[0],
        'confirmed_price': round(prices[0], 2),
        'confirm_lag_days': 0,
    }

    sorted_lows = sorted(lows, key=lambda x: (x.get('confirmed_index', x['index']), x['index']))
    for low in sorted_lows:
        if low['index'] == 0:
            return sorted_lows

    sorted_lows.insert(0, anchor)
    return sorted_lows


def _merge_nearby_highs(highs, min_gap_days):
    if not highs:
        return []
    merged = []
    for high in sorted(highs, key=lambda x: x['index']):
        if not merged or high['index'] - merged[-1]['index'] >= min_gap_days:
            merged.append(high)
            continue

        prev = merged[-1]
        prev_strength = prev['rise_pct'] + prev['pullback_pct']
        curr_strength = high['rise_pct'] + high['pullback_pct']
        if high['price'] > prev['price'] or (high['price'] == prev['price'] and curr_strength > prev_strength):
            merged[-1] = high
    return merged


def find_downtrend_highs(dates, prices, market_states, config):
    """
    实时确认价格结构高点，不按市场状态分段。

    只按时间向前推进，不看确认日之后的数据：
    1. 当前价格相对已出现的前低反弹达到阈值，开始记录候选高点；
    2. 候选高点如果继续创新高，就实时更新；
    3. 只有后面价格从候选高点回落达到阈值时，才在当天确认该高点；
    4. 返回的高点包含 confirmed_index/confirmed_date，下降压力线只能从确认日之后使用。
    """
    highs = []
    if not dates or len(prices) < 3:
        return highs

    min_rise_pct = config.get('min_rise_pct', 5.0)
    min_pullback_pct = config.get('min_pullback_pct', 5.0)
    min_confirm_days = max(1, int(config.get('pivot_window', 3)))
    low_lookback = max(min_confirm_days, int(config.get('pivot_high_lookback', 60)))
    min_high_gap_days = max(1, int(config.get('min_high_gap_days', 8)))

    candidate = None
    last_confirmed_high_index = None

    for idx, price in enumerate(prices):
        state = market_states[idx] if idx < len(market_states) else ''
        lookback_start = max(0, idx - low_lookback)
        prev_low_idx = lookback_start
        prev_low_price = prices[lookback_start]
        for scan_idx in range(lookback_start, idx + 1):
            if prices[scan_idx] <= prev_low_price:
                prev_low_price = prices[scan_idx]
                prev_low_idx = scan_idx

        if price > prev_low_price:
            rise_pct = (price - prev_low_price) / prev_low_price * 100
            if rise_pct >= min_rise_pct:
                if candidate is None or price > candidate['price']:
                    candidate = {
                        'index': idx,
                        'date': dates[idx],
                        'price': price,
                        'rise_pct': rise_pct,
                        'pullback_pct': 0.0,
                        'state': state,
                        'prev_low_index': prev_low_idx,
                        'prev_low_date': dates[prev_low_idx],
                        'prev_low_price': prev_low_price,
                    }

        if candidate is None:
            continue

        if price > candidate['price']:
            rise_pct = (price - prev_low_price) / prev_low_price * 100 if price > prev_low_price else candidate['rise_pct']
            candidate.update({
                'index': idx,
                'date': dates[idx],
                'price': price,
                'rise_pct': rise_pct,
                'pullback_pct': 0.0,
                'state': state,
                'prev_low_index': prev_low_idx,
                'prev_low_date': dates[prev_low_idx],
                'prev_low_price': prev_low_price,
            })
            continue

        pullback_pct = (candidate['price'] - price) / candidate['price'] * 100
        enough_delay = idx - candidate['index'] >= min_confirm_days
        enough_gap = (
            last_confirmed_high_index is None
            or candidate['index'] - last_confirmed_high_index >= min_high_gap_days
        )
        if pullback_pct >= min_pullback_pct and enough_delay:
            if enough_gap:
                confirmed_high = dict(candidate)
                confirmed_high.update({
                    'pullback_pct': pullback_pct,
                    'confirmed_index': idx,
                    'confirmed_date': dates[idx],
                    'confirmed_price': price,
                    'confirm_lag_days': idx - candidate['index'],
                })
                highs.append(confirmed_high)
                last_confirmed_high_index = candidate['index']
            candidate = None

    return highs

def calculate_slope_angle(price1, price2, days_diff, price_scale=1.0):
    """
    计算趋势线的斜率角度（支持正负角度）
    
    使用归一化的方式计算角度，使不同价格范围的股票都有可比性
    斜率 = ((price2 - price1) / price1) / days_diff * 100  (每日百分比变化)
    角度 = arctan(斜率 * scale) * 180 / π
    
    正角度表示上升趋势，负角度表示下降趋势
    
    Args:
        price1: 起始价格
        price2: 结束价格
        days_diff: 天数差
        price_scale: 价格缩放因子，用于调整角度敏感度
    
    Returns:
        带符号的角度值（正或负）
    """
    if days_diff == 0:
        return 0
    
    # 计算价格变化百分比
    price_change_pct = (price2 - price1) / price1 * 100
    
    # 计算每日平均变化百分比
    daily_change_pct = price_change_pct / days_diff
    
    # 使用缩放因子调整角度
    slope = daily_change_pct * price_scale
    
    # 返回带符号的角度（不取绝对值）
    angle = math.atan(slope) * 180 / math.pi
    return angle


def _state_range_is_valid(market_states, start_idx, end_idx, valid_states):
    for idx in range(start_idx, end_idx + 1):
        if idx >= len(market_states) or market_states[idx] not in valid_states:
            return False
    return True


def _first_break_index(line, dates, prices, market_states, valid_states, config):
    start_idx = line['start']['index']
    start_price = line['start']['price']
    slope = line['slope']
    active_idx = line.get('active', line['end'])['index']
    tolerance_pct = config.get('break_tolerance_pct', 0.8)

    direction = line.get('direction', 'up')
    for idx in range(active_idx + 1, len(prices)):
        line_price = _line_price(start_idx, start_price, slope, idx)
        if _line_is_broken_by_price(direction, prices[idx], line_price, config)[0]:
            return idx
    return None


def _line_is_valid_before_activation(line, prices, market_states, valid_states, config):
    start_idx = line['start']['index']
    active_idx = line['active']['index']
    start_price = line['start']['price']
    slope = line['slope']
    direction = line.get('direction', 'up')

    for idx in range(start_idx + 1, active_idx + 1):
        if market_states[idx] not in valid_states:
            return False
        line_price = _line_price(start_idx, start_price, slope, idx)
        if _line_is_broken_by_price(direction, prices[idx], line_price, config)[0]:
            return False
    return True


def _collect_touch_marks(line, dates, prices, draw_end_idx, config, mark_start_idx=None):
    start_idx = line['start']['index']
    end_idx = line['end']['index']
    active_idx = line.get('active', line['end'])['index']
    start_price = line['start']['price']
    slope = line['slope']
    direction = line.get('direction', 'up')
    tolerance_pct = config.get('touch_tolerance_pct', 1.2)
    min_gap_days = max(1, int(config.get('min_touch_gap_days', 5)))
    max_marks = max(2, int(config.get('max_touch_marks', 8)))
    first_mark_idx = active_idx if mark_start_idx is None else mark_start_idx

    touch_marks = []
    last_mark_idx = None
    for idx in range(first_mark_idx, draw_end_idx + 1):
        line_price = _line_price(start_idx, start_price, slope, idx)
        distance_pct = abs(prices[idx] - line_price) / line_price * 100 if line_price else 0
        is_anchor = idx in (start_idx, end_idx, active_idx)
        is_local_pivot = False
        if 0 < idx < len(prices) - 1:
            if direction == 'down':
                is_local_pivot = prices[idx] >= prices[idx - 1] and prices[idx] >= prices[idx + 1]
            else:
                is_local_pivot = prices[idx] <= prices[idx - 1] and prices[idx] <= prices[idx + 1]

        if not is_anchor and (distance_pct > tolerance_pct or not is_local_pivot):
            continue
        if last_mark_idx is not None and idx - last_mark_idx < min_gap_days and not is_anchor:
            continue

        touch_marks.append({
            'index': idx,
            'date': dates[idx],
            'price': round(prices[idx], 2),
            'line_price': round(line_price, 2),
            'distance_pct': round(distance_pct, 2)
        })
        last_mark_idx = idx

    if len(touch_marks) <= max_marks:
        return touch_marks

    first = touch_marks[0]
    last = touch_marks[-1]
    middle = touch_marks[1:-1]
    keep_middle_count = max_marks - 2
    if keep_middle_count <= 0:
        return [first, last]
    step = max(1, math.ceil(len(middle) / keep_middle_count))
    sampled = middle[::step][:keep_middle_count]
    return [first] + sampled + [last]


def _line_is_broken_by_price(direction, price, line_price, config):
    if line_price <= 0:
        return False, False
    tolerance_pct = config.get('break_tolerance_pct', 0.8)
    jump_pct = config.get('break_jump_pct', 3.0)
    if direction == 'down':
        break_pct = (price - line_price) / line_price * 100
    else:
        break_pct = (line_price - price) / line_price * 100

    if break_pct <= 0:
        return False, False

    is_broken = break_pct >= tolerance_pct
    is_jump_break = break_pct >= jump_pct
    return is_broken, is_jump_break


def _line_has_prior_break(line, check_idx, prices, config):
    start_idx = line['start']['index']
    end_idx = line['end']['index']
    start_price = line['start']['price']
    slope = line['slope']
    direction = line.get('direction', 'up')
    break_confirm_days = max(1, int(config.get('break_confirm_days', 1)))
    break_count = 0

    for idx in range(start_idx + 1, check_idx + 1):
        if idx == end_idx:
            continue
        line_price = _line_price(start_idx, start_price, slope, idx)
        is_broken, is_jump_break = _line_is_broken_by_price(direction, prices[idx], line_price, config)
        if is_broken or is_jump_break:
            break_count += 1
            if is_jump_break:
                return True
            if break_count >= break_confirm_days:
                return True
        else:
            break_count = 0
    return False

def _build_realtime_line(low1, low2, check_idx, dates, prices, config):
    start_idx = low1['index']
    end_idx = low2['index']
    anchor_days = end_idx - start_idx
    if anchor_days <= 0 or check_idx <= end_idx:
        return None
    if anchor_days <= 0:
        return None

    min_low_rise_pct = config.get('min_low_rise_pct', 0.5)
    min_angle = config['min_slope_angle']
    max_angle = config['max_slope_angle']
    target_angle = config.get('target_slope_angle', 50)
    price_scale = config.get('price_scale', 2.0)
    allow_negative = config.get('allow_negative_slope', False)

    confirmed_idx = low2.get('confirmed_index', end_idx)
    if confirmed_idx > check_idx:
        return None
    if low2['price'] <= low1['price'] * (1 + min_low_rise_pct / 100):
        return None

    angle = calculate_slope_angle(low1['price'], low2['price'], anchor_days, price_scale)
    if not allow_negative and angle <= 0:
        return None
    if not (min_angle <= abs(angle) <= max_angle):
        return None

    slope = (low2['price'] - low1['price']) / anchor_days
    active_price = _line_price(start_idx, low1['price'], slope, check_idx)
    if _line_is_broken_by_price('up', prices[check_idx], active_price, config)[0]:
        return None

    active_distance_pct = abs(prices[check_idx] - active_price) / active_price * 100 if active_price else 0
    line = {
        'direction': 'up',
        'start': low1,
        'end': low2,
        'active': {
            'index': check_idx,
            'date': dates[check_idx],
            'price': round(active_price, 2)
        },
        'angle': angle,
        'slope': slope,
        'anchor_days': anchor_days,
        'created_index': check_idx,
        'created_date': dates[check_idx],
        'active_distance_pct': round(active_distance_pct, 2),
    }

    if _line_has_prior_break(line, check_idx, prices, config):
        return None

    touch_marks_at_creation = _collect_touch_marks(line, dates, prices, check_idx, config, mark_start_idx=start_idx)
    recency_score = low2['index'] * 0.03
    distance_penalty = active_distance_pct * 3
    angle_penalty = abs(abs(angle) - target_angle) * 2
    line['creation_score'] = len(touch_marks_at_creation) * 25 + recency_score - distance_penalty - angle_penalty
    return line


def _select_realtime_line(confirmed_lows, check_idx, dates, prices, config, used_pairs, used_end_indices=None, anchor_rearm=None):
    candidates = []
    used_end_indices = used_end_indices or set()
    anchor_rearm = anchor_rearm or {}
    available_lows = [low for low in confirmed_lows if low.get('confirmed_index', low['index']) <= check_idx]
    if len(available_lows) < 2:
        return None

    for i in range(len(available_lows) - 1):
        low1 = available_lows[i]
        rearm_from = anchor_rearm.get(low1["index"], 0)
        for low2 in available_lows[i + 1:]:
            if low2.get("confirmed_index", low2["index"]) < rearm_from:
                continue
            if low2['index'] in used_end_indices:
                continue
            pair_key = (low1['index'], low2['index'])
            if pair_key in used_pairs:
                continue
            line = _build_realtime_line(low1, low2, check_idx, dates, prices, config)
            if line is not None:
                candidates.append(line)

    if not candidates:
        return None
    candidates.sort(key=lambda x: x['creation_score'], reverse=True)
    return candidates[0]


def _finish_realtime_line(line, draw_end_idx, broken, dates, prices, config):
    draw_end_price = _line_price(line['start']['index'], line['start']['price'], line['slope'], draw_end_idx)
    break_info = None
    if broken:
        break_info = {
            'index': draw_end_idx,
            'date': dates[draw_end_idx],
            'price': round(prices[draw_end_idx], 2),
            'line_price': round(draw_end_price, 2)
        }

    line.pop('pending_break', None)
    line.update({
        'draw_end': {
            'index': draw_end_idx,
            'date': dates[draw_end_idx],
            'price': round(draw_end_price, 2)
        },
        'angle': round(line['angle'], 2),
        'duration_days': draw_end_idx - line['active']['index'],
        'touch_marks': _collect_touch_marks(line, dates, prices, draw_end_idx, config, mark_start_idx=line['start']['index']),
        'broken': broken,
        'break': break_info,
        'realtime': True,
        'score': line['creation_score']
    })
    return line


def generate_trend_lines(lows, dates, prices, market_states, config):
    """
    生成可持续追加的实时上升趋势线。

    新低点确认后可以继续和之前低点组合出新的辅助趋势线，已有趋势线不会阻塞新线。
    每条线独立延伸，跌破后只截断这一条线；同一对锚点、同一个第二锚点不会重复画。
    """
    trend_lines = []
    if len(lows) < 2 or not dates:
        return trend_lines

    max_trend_lines = max(1, int(config.get('max_trend_lines', 50)))
    confirm_interval_days = max(1, int(config.get('confirm_interval_days', 5)))
    min_active_line_days = max(0, int(config.get('min_active_line_days', 10)))
    break_confirm_days = max(1, int(config.get('break_confirm_days', 1)))
    show_broken_lines = bool(config.get('show_broken_lines', True))
    confirmed_lows = sorted(lows, key=lambda x: (x.get('confirmed_index', x['index']), x['index']))

    active_lines = []
    used_pairs = set()
    used_end_indices = set()
    anchor_rearm = {}

    for idx in range(len(prices)):
        if active_lines:
            still_active = []
            for line in active_lines:
                line_price = _line_price(line['start']['index'], line['start']['price'], line['slope'], idx)
                is_broken = False
                is_jump_break = False
                if idx > line['active']['index']:
                    is_broken, is_jump_break = _line_is_broken_by_price('up', prices[idx], line_price, config)
                if is_broken:
                    pending = line.setdefault('pending_break', {'count': 0})
                    pending['count'] += 1
                    pending.setdefault('first_index', idx)
                    pending.setdefault('first_date', dates[idx])
                    pending.setdefault('first_price', round(prices[idx], 2))
                    pending.setdefault('first_line_price', round(line_price, 2))
                    if is_jump_break or pending['count'] >= break_confirm_days:
                        anchor_rearm[line["start"]["index"]] = idx
                        if idx - line['active']['index'] >= min_active_line_days and show_broken_lines:
                            trend_lines.append(_finish_realtime_line(line, idx, True, dates, prices, config))
                            if len(trend_lines) >= max_trend_lines:
                                trend_lines.sort(key=lambda x: (x['active']['index'], x['start']['index'], x['end']['index']))
                                return trend_lines[:max_trend_lines]
                        else:
                            used_end_indices.discard(line['end']['index'])
                        continue
                else:
                    line.pop('pending_break', None)
                still_active.append(line)
            active_lines = still_active

        if len(trend_lines) + len(active_lines) >= max_trend_lines:
            continue

        has_new_confirmed_low = any(
            low.get('confirmed_index', low['index']) == idx and low['index'] not in used_end_indices
            for low in confirmed_lows
        )
        if not has_new_confirmed_low and idx % confirm_interval_days != 0:
            continue

        while len(trend_lines) + len(active_lines) < max_trend_lines:
            candidate = _select_realtime_line(confirmed_lows, idx, dates, prices, config, used_pairs, used_end_indices, anchor_rearm)
            if candidate is None:
                break
            active_lines.append(candidate)
            used_pairs.add((candidate['start']['index'], candidate['end']['index']))
            used_end_indices.add(candidate['end']['index'])

    last_idx = len(prices) - 1
    for line in active_lines:
        if last_idx - line['active']['index'] >= min_active_line_days:
            trend_lines.append(_finish_realtime_line(line, last_idx, False, dates, prices, config))
            if len(trend_lines) >= max_trend_lines:
                break

    trend_lines.sort(key=lambda x: (x['active']['index'], x['start']['index'], x['end']['index']))
    return trend_lines[:max_trend_lines]


def _build_downtrend_line(high1, high2, check_idx, dates, prices, config):
    start_idx = high1['index']
    end_idx = high2['index']
    anchor_days = end_idx - start_idx
    if anchor_days <= 0 or check_idx <= end_idx:
        return None
    if anchor_days <= 0:
        return None

    min_high_decline_pct = config.get('min_high_decline_pct', 0.5)
    min_angle = config['min_slope_angle']
    max_angle = config['max_slope_angle']
    target_angle = config.get('target_slope_angle', 50)
    price_scale = config.get('price_scale', 2.0)


    confirmed_idx = high2.get('confirmed_index', end_idx)
    if confirmed_idx > check_idx:
        return None
    if not high1['price'] > high2['price']:
        return None
    if high2['price'] >= high1['price'] * (1 - min_high_decline_pct / 100):
        return None
    # 严格要求前高点必须高于后高点；只接受明显下降角度（负角，且在可用范围内）

    angle = calculate_slope_angle(high1['price'], high2['price'], anchor_days, price_scale)
    if angle >= -min_angle:
        return None
    if angle < -max_angle:
        return None

    slope = (high2['price'] - high1['price']) / anchor_days
    active_price = _line_price(start_idx, high1['price'], slope, check_idx)
    if _line_is_broken_by_price('down', prices[check_idx], active_price, config)[0]:
        return None

    active_distance_pct = abs(prices[check_idx] - active_price) / active_price * 100 if active_price else 0
    line = {
        'direction': 'down',
        'start': high1,
        'end': high2,
        'active': {
            'index': check_idx,
            'date': dates[check_idx],
            'price': round(active_price, 2)
        },
        'angle': angle,
        'slope': slope,
        'anchor_days': anchor_days,
        'created_index': check_idx,
        'created_date': dates[check_idx],
        'active_distance_pct': round(active_distance_pct, 2),
    }

    if _line_has_prior_break(line, check_idx, prices, config):
        return None

    touch_marks_at_creation = _collect_touch_marks(line, dates, prices, check_idx, config, mark_start_idx=start_idx)
    recency_score = high2['index'] * 0.03
    distance_penalty = active_distance_pct * 3
    angle_penalty = abs(abs(angle) - target_angle) * 2
    line['creation_score'] = len(touch_marks_at_creation) * 25 + recency_score - distance_penalty - angle_penalty
    return line


def _downtrend_intermediate_highs_are_aligned(line, available_highs, config):
    start_idx = line['start']['index']
    end_idx = line['end']['index']
    max_distance_pct = config.get('max_intermediate_high_distance_pct', 5.0)
    intermediate_highs = [
        high for high in available_highs
        if start_idx < high['index'] < end_idx
    ]
    if not intermediate_highs:
        return True

    for high in intermediate_highs:
        line_price = _line_price(start_idx, line['start']['price'], line['slope'], high['index'])
        distance_pct = abs(high['price'] - line_price) / line_price * 100 if line_price else 0
        if distance_pct > max_distance_pct:
            return False
    return True


def _select_downtrend_line(confirmed_highs, check_idx, dates, prices, config, used_pairs, used_end_indices=None):
    candidates = []
    used_end_indices = used_end_indices or set()
    available_highs = [high for high in confirmed_highs if high.get('confirmed_index', high['index']) <= check_idx]
    if len(available_highs) < 2:
        return None

    for i in range(len(available_highs) - 1):
        high1 = available_highs[i]
        for high2 in available_highs[i + 1:]:
            if high2['index'] in used_end_indices:
                continue
            pair_key = (high1['index'], high2['index'])
            if pair_key in used_pairs:
                continue
            line = _build_downtrend_line(high1, high2, check_idx, dates, prices, config)
            if line is not None and _downtrend_intermediate_highs_are_aligned(line, available_highs, config):
                candidates.append(line)

    if not candidates:
        return None
    candidates.sort(key=lambda x: x['creation_score'], reverse=True)
    return candidates[0]


def _finish_downtrend_line(line, draw_end_idx, broken, dates, prices, config):
    draw_end_price = _line_price(line['start']['index'], line['start']['price'], line['slope'], draw_end_idx)
    break_info = None
    if broken:
        break_info = {
            'index': draw_end_idx,
            'date': dates[draw_end_idx],
            'price': round(prices[draw_end_idx], 2),
            'line_price': round(draw_end_price, 2)
        }

    line.pop('pending_break', None)
    line.update({
        'draw_end': {
            'index': draw_end_idx,
            'date': dates[draw_end_idx],
            'price': round(draw_end_price, 2)
        },
        'angle': round(line['angle'], 2),
        'duration_days': draw_end_idx - line['active']['index'],
        'touch_marks': _collect_touch_marks(line, dates, prices, draw_end_idx, config, mark_start_idx=line['start']['index']),
        'broken': broken,
        'break': break_info,
        'realtime': True,
        'score': line['creation_score']
    })
    return line


def generate_downtrend_lines(highs, dates, prices, market_states, config):
    """
    生成可持续追加的实时下降趋势压力线。

    新高点确认后可以继续和之前高点组合出新的辅助压力线，已有压力线不会阻塞新线。
    每条线独立延伸，向上突破后只截断这一条线；同一对锚点、同一个第二锚点不会重复画。
    """
    trend_lines = []
    if len(highs) < 2 or not dates:
        return trend_lines

    max_trend_lines = max(1, int(config.get('max_trend_lines', 50)))
    confirm_interval_days = max(1, int(config.get('confirm_interval_days', 5)))
    min_active_line_days = max(0, int(config.get('min_active_line_days', 10)))
    break_confirm_days = max(1, int(config.get('break_confirm_days', 1)))
    show_broken_lines = bool(config.get('show_broken_lines', True))
    confirmed_highs = sorted(highs, key=lambda x: (x.get('confirmed_index', x['index']), x['index']))

    active_lines = []
    used_pairs = set()
    used_end_indices = set()
    anchor_rearm = {}
    anchor_rearm = {}

    for idx in range(len(prices)):
        if active_lines:
            still_active = []
            for line in active_lines:
                line_price = _line_price(line['start']['index'], line['start']['price'], line['slope'], idx)
                is_broken = False
                is_jump_break = False
                if idx > line['active']['index']:
                    is_broken, is_jump_break = _line_is_broken_by_price('down', prices[idx], line_price, config)
                if is_broken:
                    pending = line.setdefault('pending_break', {'count': 0})
                    pending['count'] += 1
                    pending.setdefault('first_index', idx)
                    pending.setdefault('first_date', dates[idx])
                    pending.setdefault('first_price', round(prices[idx], 2))
                    pending.setdefault('first_line_price', round(line_price, 2))
                    if is_jump_break or pending['count'] >= break_confirm_days:
                        if idx - line['active']['index'] >= min_active_line_days and show_broken_lines:
                            trend_lines.append(_finish_downtrend_line(line, idx, True, dates, prices, config))
                            if len(trend_lines) >= max_trend_lines:
                                trend_lines.sort(key=lambda x: (x['active']['index'], x['start']['index'], x['end']['index']))
                                return trend_lines[:max_trend_lines]
                        else:
                            used_end_indices.discard(line['end']['index'])
                        continue
                else:
                    line.pop('pending_break', None)
                still_active.append(line)
            active_lines = still_active

        if len(trend_lines) + len(active_lines) >= max_trend_lines:
            continue

        has_new_confirmed_high = any(
            high.get('confirmed_index', high['index']) == idx and high['index'] not in used_end_indices
            for high in confirmed_highs
        )
        if not has_new_confirmed_high and idx % confirm_interval_days != 0:
            continue

        while len(trend_lines) + len(active_lines) < max_trend_lines:
            candidate = _select_downtrend_line(confirmed_highs, idx, dates, prices, config, used_pairs, used_end_indices)
            if candidate is None:
                break
            active_lines.append(candidate)
            used_pairs.add((candidate['start']['index'], candidate['end']['index']))
            used_end_indices.add(candidate['end']['index'])

    last_idx = len(prices) - 1
    for line in active_lines:
        if last_idx - line['active']['index'] >= min_active_line_days:
            trend_lines.append(_finish_downtrend_line(line, last_idx, False, dates, prices, config))
            if len(trend_lines) >= max_trend_lines:
                break

    trend_lines.sort(key=lambda x: (x['active']['index'], x['start']['index'], x['end']['index']))
    return trend_lines[:max_trend_lines]


def build_trend_analysis(dates, prices, market_states):
    lows = find_uptrend_lows(dates, prices, market_states, TREND_LINE_CONFIG)
    lows = _ensure_initial_up_low_anchor(dates, prices, market_states, lows)
    up_lines = generate_trend_lines(lows, dates, prices, market_states, TREND_LINE_CONFIG)
    highs = find_downtrend_highs(dates, prices, market_states, DOWN_TREND_LINE_CONFIG)
    down_lines = generate_downtrend_lines(highs, dates, prices, market_states, DOWN_TREND_LINE_CONFIG)
    return lows, up_lines, highs, down_lines


HEAD_SHOULDER_CONFIG = {
    'shoulder_tolerance_pct': 12.0,
    'min_head_above_shoulder_pct': 8.0,
    'min_peak_gap_days': 8,
    'break_tolerance_pct': 1.0,
    'max_patterns': 12,
}


def _min_price_point(dates, prices, start_idx, end_idx):
    if start_idx > end_idx:
        return None
    min_idx = start_idx
    min_price = prices[start_idx]
    for idx in range(start_idx, end_idx + 1):
        if prices[idx] < min_price:
            min_idx = idx
            min_price = prices[idx]
    return {'index': min_idx, 'date': dates[min_idx], 'price': round(min_price, 2)}


def _neckline_price(left_valley, right_valley, idx):
    days = right_valley['index'] - left_valley['index']
    if days == 0:
        return left_valley['price']
    slope = (right_valley['price'] - left_valley['price']) / days
    return left_valley['price'] + slope * (idx - left_valley['index'])


def detect_head_shoulders(dates, prices, highs, config=None):
    config = config or HEAD_SHOULDER_CONFIG
    patterns = []
    if len(highs) < 3:
        return patterns

    shoulder_tolerance_pct = config.get('shoulder_tolerance_pct', 12.0)
    min_head_above_pct = config.get('min_head_above_shoulder_pct', 8.0)
    min_gap = config.get('min_peak_gap_days', 8)
    break_tolerance_pct = config.get('break_tolerance_pct', 1.0)
    max_patterns = max(1, int(config.get('max_patterns', 12)))
    sorted_highs = sorted(highs, key=lambda x: x['index'])
    used_break_indices = set()

    for i in range(len(sorted_highs) - 2):
        left = sorted_highs[i]
        for j in range(i + 1, len(sorted_highs) - 1):
            head = sorted_highs[j]
            if head['index'] - left['index'] < min_gap:
                continue
            for k in range(j + 1, len(sorted_highs)):
                right = sorted_highs[k]
                if right['index'] - head['index'] < min_gap:
                    continue
                shoulder_avg = (left['price'] + right['price']) / 2
                if shoulder_avg <= 0:
                    continue
                shoulder_diff_pct = abs(left['price'] - right['price']) / shoulder_avg * 100
                if shoulder_diff_pct > shoulder_tolerance_pct:
                    continue
                head_above_pct = (head['price'] - max(left['price'], right['price'])) / max(left['price'], right['price']) * 100
                if head_above_pct < min_head_above_pct:
                    continue

                left_valley = _min_price_point(dates, prices, left['index'], head['index'])
                right_valley = _min_price_point(dates, prices, head['index'], right['index'])
                if not left_valley or not right_valley or left_valley['index'] == right_valley['index']:
                    continue

                head_neck_price = _neckline_price(left_valley, right_valley, head['index'])
                measure = head['price'] - head_neck_price
                if measure <= 0:
                    continue

                break_info = None
                for idx in range(right['index'] + 1, len(prices)):
                    line_price = _neckline_price(left_valley, right_valley, idx)
                    if prices[idx] < line_price * (1 - break_tolerance_pct / 100):
                        if idx in used_break_indices:
                            break
                        target_price = line_price - measure
                        break_info = {
                            'index': idx,
                            'date': dates[idx],
                            'price': round(prices[idx], 2),
                            'line_price': round(line_price, 2),
                            'target_price': round(target_price, 2),
                        }
                        used_break_indices.add(idx)
                        break
                if not break_info:
                    continue

                draw_end_idx = min(len(prices) - 1, break_info['index'] + 30)
                draw_end_price = _neckline_price(left_valley, right_valley, draw_end_idx)
                pattern = {
                    'left_shoulder': left,
                    'head': head,
                    'right_shoulder': right,
                    'left_valley': left_valley,
                    'right_valley': right_valley,
                    'draw_end': {'index': draw_end_idx, 'date': dates[draw_end_idx], 'price': round(draw_end_price, 2)},
                    'measure': round(measure, 2),
                    'break': break_info,
                    'score': round(head_above_pct * 2 - shoulder_diff_pct + (right['index'] - left['index']) * 0.03, 2),
                }
                patterns.append(pattern)
                break
            if len(patterns) >= max_patterns:
                break
        if len(patterns) >= max_patterns:
            break

    patterns.sort(key=lambda x: (x['break']['index'], -x['score']))
    return patterns[:max_patterns]


def calculate_trend_break_markers(dates, prices, market_states):
    _, up_lines, highs, down_lines = build_trend_analysis(dates, prices, market_states)
    head_shoulders = detect_head_shoulders(dates, prices, highs, HEAD_SHOULDER_CONFIG)
    markers = {}

    for line in up_lines:
        break_info = line.get('break')
        if not break_info:
            continue
        angle = line.get('angle')
        slope = line.get('slope')
        marker = (
            f"上升趋势线跌破@{break_info['price']:.2f}/线{break_info['line_price']:.2f}"
            f"|角度:{angle:.2f}°"
            f"|斜率:{slope:.4f}"
            f"|锚点1:{line['start']['date']}@{line['start']['price']:.2f}"
            f"|锚点2:{line['end']['date']}@{line['end']['price']:.2f}"
        )
        date_markers = markers.setdefault(break_info['date'], [])
        if marker not in date_markers:
            date_markers.append(marker)

    for line in down_lines:
        break_info = line.get('break')
        if not break_info:
            continue
        angle = line.get('angle')
        slope = line.get('slope')
        marker = (
            f"下降趋势线上破@{break_info['price']:.2f}/线{break_info['line_price']:.2f}"
            f"|角度:{angle:.2f}°"
            f"|斜率:{slope:.4f}"
            f"|锚点1:{line['start']['date']}@{line['start']['price']:.2f}"
            f"|锚点2:{line['end']['date']}@{line['end']['price']:.2f}"
        )
        date_markers = markers.setdefault(break_info['date'], [])
        if marker not in date_markers:
            date_markers.append(marker)

    for pattern in head_shoulders:
        break_info = pattern.get('break')
        if not break_info:
            continue
        marker = (
            f"头肩颈线跌破@{break_info['price']:.2f}"
            f"/颈线{break_info['line_price']:.2f}"
            f"/目标{break_info['target_price']:.2f}"
        )
        date_markers = markers.setdefault(break_info['date'], [])
        if marker not in date_markers:
            date_markers.append(marker)

    return markers


def annotate_output_file_with_trend_breaks(filepath='out_put.txt'):
    if not os.path.exists(filepath):
        return {}

    with open(filepath, 'r', encoding='utf-8') as f:
        raw_lines = f.readlines()

    cleaned_lines = [_strip_trend_markers(line.rstrip('\n')) for line in raw_lines]
    dates, prices, market_states, _, _ = _parse_output_lines(cleaned_lines)
    markers = calculate_trend_break_markers(dates, prices, market_states)
    if not markers:
        if cleaned_lines != [line.rstrip('\n') for line in raw_lines]:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(cleaned_lines))
        return markers

    trend_event_markers = {
        "上升趋势线跌破卖出@",
        "上升趋势线跌破卖出",
        "下降趋势线上破买入@",
        "下降趋势线上破买入",
    }

    annotated_lines = []
    for line in cleaned_lines:
        parts = line.split()
        if len(parts) >= 10 and parts[1] in markers:
            # 只给买入/卖出事件行补充破位标记，避免在止损或普通K线行误填充历史锚点
            if any(tag in line for tag in trend_event_markers):
                line = f"{line} 趋势破位:{';'.join(markers[parts[1]])}"
        annotated_lines.append(line)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(annotated_lines))
    return markers

def generate_standalone_html(dates, prices, market_states, buy_points, sell_points, output_path, lows=None, trend_lines=None, highs=None, down_trend_lines=None, head_shoulders=None):
    # 读取echarts库文件
    try:
        with open('echarts.min.js', 'r', encoding='utf-8') as f:
            echarts_code = f.read()
    except:
        print("警告: 未找到echarts.min.js文件，将使用CDN链接")
        echarts_code = None
    
    # 生成市场状态区域
    state_areas = generate_market_state_areas(dates, market_states)
    
    # 识别上升趋势低点/支撑线、下降趋势高点/压力线，以及头肩顶颈线
    if lows is None or trend_lines is None or highs is None or down_trend_lines is None:
        calc_lows, calc_trend_lines, calc_highs, calc_down_trend_lines = build_trend_analysis(dates, prices, market_states)
        if lows is None:
            lows = calc_lows
        if trend_lines is None:
            trend_lines = calc_trend_lines
        if highs is None:
            highs = calc_highs
        if down_trend_lines is None:
            down_trend_lines = calc_down_trend_lines
    if head_shoulders is None:
        head_shoulders = detect_head_shoulders(dates, prices, highs, HEAD_SHOULDER_CONFIG)
    
    buy_data_json = json.dumps(buy_points, ensure_ascii=False)
    sell_data_json = json.dumps(sell_points, ensure_ascii=False)
    dates_json = json.dumps(dates, ensure_ascii=False)
    prices_json = json.dumps(prices, ensure_ascii=False)
    states_json = json.dumps(market_states, ensure_ascii=False)
    state_areas_json = json.dumps(state_areas, ensure_ascii=False)
    state_colors_json = json.dumps(STATE_COLORS, ensure_ascii=False)
    lows_json = json.dumps(lows, ensure_ascii=False)
    trend_lines_json = json.dumps(trend_lines, ensure_ascii=False)
    highs_json = json.dumps(highs, ensure_ascii=False)
    down_trend_lines_json = json.dumps(down_trend_lines, ensure_ascii=False)
    head_shoulders_json = json.dumps(head_shoulders, ensure_ascii=False)
    
    # 如果echarts代码存在，内嵌它；否则使用CDN
    if echarts_code:
        echarts_script = f'<script>{echarts_code}</script>'
    else:
        echarts_script = '<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>'
    
    # 生成状态图例HTML
    state_legend_html = ''.join([
        f'<div class="state-legend-item"><div class="state-legend-color" style="background:{color}"></div><span>{state}</span></div>'
        for state, color in STATE_COLORS.items()
    ])
    
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>股票交易走势图 - 603083</title>
    {echarts_script}
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }}
        .container {{ max-width: 1400px; margin: 0 auto; background: white; border-radius: 16px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); overflow: hidden; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center; }}
        .header h1 {{ font-size: 24px; margin-bottom: 5px; }}
        .stats {{ display: flex; justify-content: center; gap: 40px; margin-top: 15px; flex-wrap: wrap; }}
        .stat-item {{ text-align: center; }}
        .stat-value {{ font-size: 20px; font-weight: bold; }}
        .stat-label {{ font-size: 12px; opacity: 0.8; }}
        .legend {{ display: flex; justify-content: center; gap: 30px; padding: 15px; background: #f8f9fa; border-bottom: 1px solid #e9ecef; flex-wrap: wrap; }}
        .legend-item {{ display: flex; align-items: center; gap: 6px; font-size: 13px; color: #495057; }}
        .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; }}
        .buy-dot {{ background: #ff4757; }}
        .sell-dot {{ background: #2ed573; }}
        .state-legend {{ display: flex; justify-content: center; gap: 15px; padding: 10px; background: #fff; border-bottom: 1px solid #e9ecef; flex-wrap: wrap; }}
        .state-legend-item {{ display: flex; align-items: center; gap: 4px; font-size: 11px; color: #666; }}
        .state-legend-color {{ width: 20px; height: 12px; border-radius: 2px; }}
        .manual-tools {{ display: flex; align-items: center; gap: 8px; padding: 10px 16px; background: #f8f9fa; border-bottom: 1px solid #e9ecef; flex-wrap: wrap; }}
        .manual-tools select {{ border: 1px solid #ced4da; background: #fff; color: #343a40; border-radius: 6px; padding: 6px 10px; font-size: 12px; }}
        .manual-tools button {{ border: 1px solid #ced4da; background: #fff; color: #343a40; border-radius: 6px; padding: 6px 10px; cursor: pointer; font-size: 12px; }}
        .manual-tools button.active {{ background: #2f80ed; border-color: #2f80ed; color: #fff; }}
        .manual-tools button:disabled {{ opacity: 0.45; cursor: not-allowed; }}
        .manual-status {{ color: #495057; font-size: 12px; min-width: 260px; }}
        #chart {{ width: 100%; height: 650px; padding: 15px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>股票交易走势图 - 603083</h1>
            <p>回测区间: 2020-2026</p>
            <div class="stats">
                <div class="stat-item"><div class="stat-value">{len(dates)}</div><div class="stat-label">交易日</div></div>
                <div class="stat-item"><div class="stat-value">{len(buy_points)}</div><div class="stat-label">买入次数</div></div>
                <div class="stat-item"><div class="stat-value">{len(sell_points)}</div><div class="stat-label">卖出次数</div></div>
                <div class="stat-item"><div class="stat-value">+473%</div><div class="stat-label">总收益率</div></div>
            </div>
        </div>
        <div class="legend">
            <div class="legend-item"><div class="legend-dot buy-dot"></div><span>买入点 (红色)</span></div>
            <div class="legend-item"><div class="legend-dot sell-dot"></div><span>卖出点 (绿色)</span></div>
        </div>
        <div class="state-legend">
            {state_legend_html}
        </div>
        <div class="manual-tools">
            <button id="manualDrawBtn" type="button">手动画线</button>
            <select id="manualLineMode"><option value="trend">趋势线</option><option value="neckline">颈线</option></select>
            <span id="manualDrawStatus" class="manual-status">点击“手动画线”后，在图上按住拖动划线，松开后自动向右延长</span>
        </div>
        <div id="chart"></div>
    </div>
    <script>
        const dates = {dates_json};
        const prices = {prices_json};
        const marketStates = {states_json};
        const stateAreas = {state_areas_json};
        const stateColors = {state_colors_json};
        const buyPoints = {buy_data_json};
        const sellPoints = {sell_data_json};
        const uptrendLows = {lows_json};
        const trendLines = {trend_lines_json};
        const downtrendHighs = {highs_json};
        const downTrendLines = {down_trend_lines_json};
        const headShoulders = {head_shoulders_json};
        
        // 生成 markArea 数据
        const markAreaData = stateAreas.map(area => ({{
            xAxis: area.start_date,
            xAxisEnd: area.end_date,
            itemStyle: {{ color: area.color, opacity: 0.15 }},
            label: {{
                show: true,
                position: 'insideBottom',
                formatter: area.state,
                fontSize: 10,
                color: area.color,
                fontWeight: 'bold'
            }}
        }}));
        
        const buyMarks = buyPoints.map(p => ({{
            coord: [p.date, p.price],
            value: '买',
            itemStyle: {{ color: '#ff4757' }},
            label: {{ show: true, formatter: '买', color: '#fff', fontSize: 11, fontWeight: 'bold' }}
        }}));
        
        const sellMarks = sellPoints.map(p => ({{
            coord: [p.date, p.price],
            value: '卖',
            itemStyle: {{ color: '#2ed573' }},
            label: {{ show: true, formatter: '卖', color: '#fff', fontSize: 11, fontWeight: 'bold' }}
        }}));
        
        // 生成上升趋势低点的标记
        const lowMarks = uptrendLows.map(low => ({{
            coord: [low.date, low.price],
            value: '低',
            itemStyle: {{ color: '#ffa502' }},
            label: {{ show: true, formatter: '低', color: '#fff', fontSize: 9, fontWeight: 'bold' }}
        }}));

        // 生成下降趋势高点的标记
        const highMarks = downtrendHighs.map(high => ({{
            coord: [high.date, high.price],
            value: '高',
            itemStyle: {{ color: '#8e44ad' }},
            label: {{ show: true, formatter: '高', color: '#fff', fontSize: 9, fontWeight: 'bold' }}
        }}));

        function createTrendLineSeries(lines, options) {{
            return lines.map((line, idx) => {{
                const anchorPoint = line.end;
                const activePoint = line.active || anchorPoint;
                const endPoint = line.draw_end || activePoint;
                const realtimeData = [
                    [line.start.date, line.start.price],
                    [anchorPoint.date, anchorPoint.price]
                ];
                if (endPoint.date !== anchorPoint.date) {{
                    realtimeData.push([endPoint.date, endPoint.price]);
                }}

                const touchMarks = (line.touch_marks || []).map(t => ({{
                    coord: [t.date, t.line_price],
                    value: t.price.toFixed(2),
                    itemStyle: {{ color: options.color }},
                    label: {{
                        show: true,
                        formatter: t.price.toFixed(2),
                        color: options.color,
                        fontSize: 10,
                        fontWeight: 'bold',
                        position: options.touchPosition
                    }},
                    symbol: 'circle',
                    symbolSize: 8
                }}));

                if (line.active) {{
                    touchMarks.push({{
                        coord: [line.active.date, line.active.price],
                        value: '确认 ' + line.active.price.toFixed(2),
                        itemStyle: {{ color: '#1e90ff' }},
                        label: {{
                            show: true,
                            formatter: '确认 ' + line.active.price.toFixed(2),
                            color: '#1e90ff',
                            fontSize: 10,
                            fontWeight: 'bold',
                            position: 'top'
                        }},
                        symbol: 'diamond',
                        symbolSize: 12
                    }});
                }}

                if (line.break) {{
                    const breakText = options.breakText + ' ' + line.break.price.toFixed(2);
                    touchMarks.push({{
                        coord: [line.break.date, line.break.line_price],
                        value: breakText,
                        itemStyle: {{ color: options.breakColor }},
                        label: {{
                            show: true,
                            formatter: breakText,
                            color: options.breakColor,
                            fontSize: 10,
                            fontWeight: 'bold',
                            position: options.breakPosition
                        }},
                        symbol: 'pin',
                        symbolSize: 48
                    }});
                }}

                return {{
                    name: options.name,
                    type: 'line',
                    data: realtimeData,
                    lineStyle: {{ color: line.broken ? options.brokenColor : options.color, width: 2.5, type: 'dashed' }},
                    symbol: 'none',
                    smooth: false,
                    silent: false,
                    tooltip: {{
                        formatter: function() {{
                            const status = line.broken ? options.brokenStatus : '有效';
                            const activeText = line.active ? '<br>确认: ' + line.active.date + ' @' + line.active.price.toFixed(2) : '';
                            return options.name + (idx + 1) + '<br>状态: ' + status + '<br>锚点1: ' + line.start.date + ' @' + line.start.price.toFixed(2) + '<br>锚点2: ' + line.end.date + ' @' + line.end.price.toFixed(2) + activeText + '<br>角度: ' + line.angle.toFixed(1) + '°<br>确认后运行: ' + line.duration_days + '个交易日';
                        }}
                    }},
                    markPoint: {{ data: touchMarks }}
                }};
            }});
        }}

        const upTrendLineSeries = createTrendLineSeries(trendLines, {{
            name: '上升趋势线',
            color: '#ff6348',
            brokenColor: '#747d8c',
            breakColor: '#2f3542',
            breakText: '跌破',
            breakPosition: 'bottom',
            touchPosition: 'top',
            brokenStatus: '已跌破'
        }});

        const downTrendLineSeries = createTrendLineSeries(downTrendLines, {{
            name: '下降趋势线',
            color: '#2ed573',
            brokenColor: '#747d8c',
            breakColor: '#3742fa',
            breakText: '上破',
            breakPosition: 'top',
            touchPosition: 'bottom',
            brokenStatus: '已上破'
        }});

        function createHeadShoulderSeries(patterns) {{
            return patterns.map((pattern, idx) => {{
                const breakInfo = pattern.break;
                const markData = [
                    {{ coord: [pattern.left_shoulder.date, pattern.left_shoulder.price], value: '左肩', itemStyle: {{ color: '#6c5ce7' }}, label: {{ show: true, formatter: '左肩', color: '#6c5ce7', fontSize: 10, fontWeight: 'bold', position: 'top' }}, symbol: 'triangle', symbolSize: 12 }},
                    {{ coord: [pattern.head.date, pattern.head.price], value: '头', itemStyle: {{ color: '#d63031' }}, label: {{ show: true, formatter: '头', color: '#d63031', fontSize: 10, fontWeight: 'bold', position: 'top' }}, symbol: 'triangle', symbolSize: 14 }},
                    {{ coord: [pattern.right_shoulder.date, pattern.right_shoulder.price], value: '右肩', itemStyle: {{ color: '#6c5ce7' }}, label: {{ show: true, formatter: '右肩', color: '#6c5ce7', fontSize: 10, fontWeight: 'bold', position: 'top' }}, symbol: 'triangle', symbolSize: 12 }},
                    {{ coord: [breakInfo.date, breakInfo.line_price], value: '目标', itemStyle: {{ color: '#e17055' }}, label: {{ show: true, formatter: '跌破 ' + breakInfo.price.toFixed(2) + '\\n目标 ' + breakInfo.target_price.toFixed(2), color: '#e17055', fontSize: 10, fontWeight: 'bold', position: 'bottom' }}, symbol: 'pin', symbolSize: 58 }}
                ];
                return {{
                    name: '头肩颈线',
                    type: 'line',
                    data: [
                        [pattern.left_valley.date, pattern.left_valley.price],
                        [pattern.right_valley.date, pattern.right_valley.price],
                        [pattern.draw_end.date, pattern.draw_end.price]
                    ],
                    symbol: 'none',
                    smooth: false,
                    lineStyle: {{ color: '#e17055', width: 2.4, type: 'dotted' }},
                    tooltip: {{ formatter: function() {{ return '头肩颈线' + (idx + 1) + '<br>头部: ' + pattern.head.date + ' @' + pattern.head.price.toFixed(2) + '<br>颈线跌破: ' + breakInfo.date + ' @' + breakInfo.price.toFixed(2) + '<br>量度跌幅: ' + pattern.measure.toFixed(2) + '<br>目标价: ' + breakInfo.target_price.toFixed(2); }} }},
                    markPoint: {{ data: markData }}
                }};
            }});
        }}

        const headShoulderSeries = createHeadShoulderSeries(headShoulders);
        
        const chart = echarts.init(document.getElementById('chart'));
        const option = {{
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    const dataIndex = params[0].dataIndex;
                    const state = marketStates[dataIndex] || '';
                    let r = '日期: ' + params[0].axisValue + '<br>收盘价: ' + params[0].data + '<br>市场状态: ' + state;
                    const d = params[0].axisValue;
                    buyPoints.forEach(p => {{ if(p.date === d) r += '<br><span style="color:#ff4757;font-weight:bold;">买入 @' + p.price + '<br>类型: ' + p.type + '</span>'; }});
                    sellPoints.forEach(p => {{ if(p.date === d) r += '<br><span style="color:#2ed573;font-weight:bold;">卖出 @' + p.price + '<br>盈亏: ' + p.profit + ' (' + p.profit_pct + '%)</span>'; }});
                    // 检查是否是低点/高点
                    const low = uptrendLows.find(l => l.date === d);
                    if (low) {{
                        r += '<br><span style="color:#ffa502;font-weight:bold;">上升趋势低点 @' + low.price.toFixed(2) + '<br>确认日: ' + (low.confirmed_date || '-') + '<br>跌幅: ' + low.decline_pct.toFixed(1) + '%<br>确认反弹: ' + low.rebound_pct.toFixed(1) + '%</span>';
                    }}
                    const high = downtrendHighs.find(h => h.date === d);
                    if (high) {{
                        r += '<br><span style="color:#8e44ad;font-weight:bold;">下降趋势高点 @' + high.price.toFixed(2) + '<br>确认日: ' + (high.confirmed_date || '-') + '<br>涨幅: ' + high.rise_pct.toFixed(1) + '%<br>确认回落: ' + high.pullback_pct.toFixed(1) + '%</span>';
                    }}
                    return r;
                }}
            }},
            legend: {{
                data: ['收盘价', '上升趋势线', '下降趋势线'],
                top: 5,
                textStyle: {{ fontSize: 11 }}
            }},
            grid: {{ left: '3%', right: '4%', bottom: '15%', top: '15%', containLabel: true }},
            xAxis: {{ 
                type: 'category', 
                data: dates, 
                boundaryGap: false, 
                axisLabel: {{ rotate: 45, fontSize: 9 }},
                axisLine: {{ lineStyle: {{ color: '#999' }} }}
            }},
            yAxis: {{ 
                type: 'value', 
                scale: true, 
                splitLine: {{ lineStyle: {{ type: 'dashed', color: '#eee' }} }},
                axisLine: {{ lineStyle: {{ color: '#999' }} }}
            }},
            dataZoom: [
                {{ type: 'inside', start: 0, end: 100 }}, 
                {{ type: 'slider', start: 0, end: 100, height: 30, bottom: 50 }}
            ],
            series: []
        }};

        const baseSeries = [
            {{
                name: '收盘价',
                type: 'line',
                data: prices,
                smooth: true,
                symbol: 'none',
                lineStyle: {{ color: '#5470c6', width: 2 }},
                areaStyle: {{ color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{{offset: 0, color: 'rgba(84,112,198,0.3)'}}, {{offset: 1, color: 'rgba(84,112,198,0.05)'}}] }} }},
                markPoint: {{ data: [...buyMarks, ...sellMarks, ...lowMarks, ...highMarks], symbol: 'pin', symbolSize: 45, label: {{ fontSize: 10 }} }},
                markArea: {{
                    silent: true,
                    data: markAreaData.map(area => [{{
                        xAxis: area.xAxis,
                        itemStyle: area.itemStyle,
                        label: area.label
                    }}, {{
                        xAxis: area.xAxisEnd
                    }}])
                }}
            }},
            ...upTrendLineSeries,
            ...downTrendLineSeries,
            ...headShoulderSeries
        ];
        const manualDrawBtn = document.getElementById('manualDrawBtn');
        const manualLineMode = document.getElementById('manualLineMode');
        const manualDrawStatus = document.getElementById('manualDrawStatus');
        let manualDrawingEnabled = false;
        let manualDragStart = null;
        let manualDraftLine = null;
        let manualLines = [];
        let manualLineId = 1;
        let selectedManualLineId = null;

        function clampIndex(rawIdx) {{
            if (typeof rawIdx === 'string') {{
                const matchedIndex = dates.indexOf(rawIdx);
                if (matchedIndex >= 0) {{
                    return matchedIndex;
                }}
            }}
            const numericIndex = Number(rawIdx);
            if (!Number.isFinite(numericIndex)) {{
                return 0;
            }}
            return Math.max(0, Math.min(dates.length - 1, Math.round(numericIndex)));
        }}

        function pixelToManualPoint(event) {{
            const pixel = [event.offsetX, event.offsetY];
            if (!chart.containPixel({{ gridIndex: 0 }}, pixel)) {{
                return null;
            }}
            const coord = chart.convertFromPixel({{ gridIndex: 0 }}, pixel);
            const idx = clampIndex(coord[0]);
            const price = Number(coord[1]);
            if (!Number.isFinite(price)) {{
                return null;
            }}
            return {{ index: idx, date: dates[idx], price }};
        }}

        function linePriceAt(line, idx) {{
            return line.start.price + line.slope * (idx - line.start.index);
        }}

        function findManualCross(line) {{
            const scanStart = Math.max(line.start.index, line.end.index) + 1;
            const direction = line.slope >= 0 ? 'up' : 'down';
            for (let idx = scanStart; idx < prices.length; idx++) {{
                const linePrice = linePriceAt(line, idx);
                const prevLinePrice = linePriceAt(line, idx - 1);
                const prevDiff = prices[idx - 1] - prevLinePrice;
                const diff = prices[idx] - linePrice;
                if (line.mode === 'neckline') {{
                    if (prevDiff >= 0 && diff < 0) {{
                        return {{ index: idx, date: dates[idx], price: prices[idx], linePrice, text: '跌破' }};
                    }}
                    continue;
                }}
                if (direction === 'up' && prevDiff >= 0 && diff < 0) {{
                    return {{ index: idx, date: dates[idx], price: prices[idx], linePrice, text: '跌破' }};
                }}
                if (direction === 'down' && prevDiff <= 0 && diff > 0) {{
                    return {{ index: idx, date: dates[idx], price: prices[idx], linePrice, text: '突破' }};
                }}
                if (prevDiff === 0 || diff === 0 || (prevDiff < 0 && diff > 0) || (prevDiff > 0 && diff < 0)) {{
                    return {{ index: idx, date: dates[idx], price: prices[idx], linePrice, text: '交汇' }};
                }}
            }}
            return null;
        }}

        function findManualHeadForNeckline(line) {{
            const scanStart = Math.max(0, Math.min(line.start.index, line.end.index) - 120);
            const scanEnd = Math.max(line.start.index, line.end.index);
            let headIdx = scanStart;
            let headPrice = prices[scanStart];
            for (let idx = scanStart; idx <= scanEnd; idx++) {{
                if (prices[idx] > headPrice) {{
                    headIdx = idx;
                    headPrice = prices[idx];
                }}
            }}
            const neckAtHead = linePriceAt(line, headIdx);
            const measure = Math.max(0, headPrice - neckAtHead);
            return {{ index: headIdx, date: dates[headIdx], price: headPrice, neckPrice: neckAtHead, measure }};
        }}

        function buildManualLine(rawStart, rawEnd, preview = false) {{
            if (!rawStart || !rawEnd || rawStart.index === rawEnd.index) {{
                return null;
            }}
            const start = rawStart.index < rawEnd.index ? rawStart : rawEnd;
            const end = rawStart.index < rawEnd.index ? rawEnd : rawStart;
            const days = end.index - start.index;
            const slope = (end.price - start.price) / days;
            const angle = Math.atan((slope / Math.max(Math.abs(start.price), 0.01)) * 100 * 2.0) * 180 / Math.PI;
            const mode = manualLineMode.value === 'neckline' ? 'neckline' : 'trend';
            const line = {{
                id: preview ? 0 : manualLineId++,
                mode,
                start,
                end,
                days,
                slope,
                angle,
                direction: slope >= 0 ? 'up' : 'down',
                breakPoint: null,
                head: null,
                targetPrice: null,
                preview
            }};
            if (mode === 'neckline') {{
                line.head = findManualHeadForNeckline(line);
            }}
            if (!preview) {{
                line.breakPoint = findManualCross(line);
                if (mode === 'neckline' && line.head) {{
                    const targetBasePrice = line.breakPoint ? line.breakPoint.linePrice : linePriceAt(line, line.end.index);
                    line.targetPrice = targetBasePrice - line.head.measure;
                }}
            }}
            return line;
        }}

        function setManualPreviewGraphic(line) {{
            if (!line) {{
                chart.setOption({{ graphic: [{{ id: 'manualPreviewLine', $action: 'remove' }}] }});
                return;
            }}
            const startPixel = chart.convertToPixel({{ gridIndex: 0 }}, [line.start.date, line.start.price]);
            const endPixel = chart.convertToPixel({{ gridIndex: 0 }}, [line.end.date, line.end.price]);
            chart.setOption({{
                graphic: [{{
                    id: 'manualPreviewLine',
                    type: 'line',
                    shape: {{ x1: startPixel[0], y1: startPixel[1], x2: endPixel[0], y2: endPixel[1] }},
                    style: {{ stroke: '#111827', lineWidth: 2, lineDash: [6, 4] }},
                    silent: true,
                    z: 100
                }}]
            }});
        }}

        function formatManualStatus() {{
            if (manualDraftLine) {{
                manualDrawStatus.textContent = (manualDraftLine.mode === 'neckline' ? '颈线' : '趋势线') + '拖动中：角度 ' + manualDraftLine.angle.toFixed(1) + '°，日斜率 ' + manualDraftLine.slope.toFixed(3);
                return;
            }}
            if (selectedManualLineId !== null) {{
                manualDrawStatus.textContent = '已选中手动画线' + selectedManualLineId + '，点击线旁 x 可删除';
                return;
            }}
            if (manualDrawingEnabled) {{
                manualDrawStatus.textContent = '画线中：当前模式 ' + (manualLineMode.value === 'neckline' ? '颈线' : '趋势线') + '，按住拖动后松开';
                return;
            }}
            manualDrawStatus.textContent = '选择趋势线或颈线，点击“手动画线”后按住拖动划线';
        }}

        function updateManualButtons() {{
            manualDrawBtn.classList.toggle('active', manualDrawingEnabled);
            chart.getZr().setCursorStyle(manualDrawingEnabled ? 'crosshair' : 'default');
            formatManualStatus();
        }}

        function manualLineToSeries(line) {{
            const extendIdx = line.breakPoint ? line.breakPoint.index : dates.length - 1;
            const extendPrice = linePriceAt(line, extendIdx);
            const isSelected = selectedManualLineId === line.id;
            const lineColor = isSelected ? '#d63031' : (line.mode === 'neckline' ? '#e17055' : '#f59f00');
            const markData = [
                {{
                    coord: [line.start.date, line.start.price],
                    value: '起点',
                    itemStyle: {{ color: lineColor }},
                    label: {{ show: true, formatter: line.start.price.toFixed(2), color: lineColor, fontSize: 10, position: 'top' }},
                    symbol: 'circle',
                    symbolSize: 10
                }},
                {{
                    coord: [line.end.date, line.end.price],
                    value: '斜率',
                    itemStyle: {{ color: lineColor }},
                    label: {{ show: true, formatter: (line.mode === 'neckline' ? '颈线' : '角度 ' + line.angle.toFixed(1) + '°') + '\\n日斜率 ' + line.slope.toFixed(3), color: lineColor, fontSize: 10, fontWeight: 'bold', position: 'top' }},
                    symbol: 'diamond',
                    symbolSize: 14
                }}
            ];
            if (isSelected) {{
                const deleteIdx = Math.round((line.end.index + (line.breakPoint ? line.breakPoint.index : Math.min(dates.length - 1, line.end.index + 12))) / 2);
                markData.push({{
                    coord: [dates[deleteIdx], linePriceAt(line, deleteIdx)],
                    value: 'delete',
                    itemStyle: {{ color: '#2d3436' }},
                    label: {{ show: true, formatter: 'x', color: '#fff', fontSize: 13, fontWeight: 'bold' }},
                    symbol: 'circle',
                    symbolSize: 22,
                    manualDeleteLineId: line.id
                }});
            }}
            if (line.mode === 'neckline' && line.head) {{
                markData.push({{
                    coord: [line.head.date, line.head.price],
                    value: '头部',
                    itemStyle: {{ color: '#d63031' }},
                    label: {{ show: true, formatter: '头部 ' + line.head.price.toFixed(2), color: '#d63031', fontSize: 10, fontWeight: 'bold', position: 'top' }},
                    symbol: 'triangle',
                    symbolSize: 14
                }});
            }}
            if (line.breakPoint) {{
                const isNeckline = line.mode === 'neckline';
                const markerText = isNeckline && line.targetPrice !== null
                    ? '跌破 ' + line.breakPoint.price.toFixed(2) + '\\n目标 ' + line.targetPrice.toFixed(2)
                    : line.breakPoint.text + ' ' + line.breakPoint.price.toFixed(2);
                markData.push({{
                    coord: [line.breakPoint.date, line.breakPoint.linePrice],
                    value: line.breakPoint.text,
                    itemStyle: {{ color: isNeckline ? '#e17055' : (line.direction === 'up' ? '#d63031' : '#0984e3') }},
                    label: {{
                        show: true,
                        formatter: markerText,
                        color: isNeckline ? '#e17055' : (line.direction === 'up' ? '#d63031' : '#0984e3'),
                        fontSize: 10,
                        fontWeight: 'bold',
                        position: line.direction === 'up' || isNeckline ? 'bottom' : 'top'
                    }},
                    symbol: 'pin',
                    symbolSize: isNeckline ? 58 : 48
                }});
            }}
            if (line.mode === 'neckline' && line.targetPrice !== null && !line.breakPoint) {{
                markData.push({{
                    coord: [line.end.date, line.end.price],
                    value: '目标',
                    itemStyle: {{ color: '#e17055' }},
                    label: {{ show: true, formatter: '目标 ' + line.targetPrice.toFixed(2), color: '#e17055', fontSize: 10, fontWeight: 'bold', position: 'bottom' }},
                    symbol: 'pin',
                    symbolSize: 52
                }});
            }}
            return {{
                name: line.mode === 'neckline' ? '手画颈线' : '手动画线',
                manualLineId: line.id,
                type: 'line',
                data: [
                    [line.start.date, line.start.price],
                    [line.end.date, line.end.price],
                    [dates[extendIdx], extendPrice]
                ],
                symbol: 'none',
                smooth: false,
                z: 20,
                lineStyle: {{ color: lineColor, width: isSelected ? 4 : 2.8, type: 'solid' }},
                tooltip: {{
                    formatter: function() {{
                        const crossText = line.breakPoint ? '<br>' + line.breakPoint.text + ': ' + line.breakPoint.date + ' @' + line.breakPoint.price.toFixed(2) + '<br>线价: ' + line.breakPoint.linePrice.toFixed(2) : '<br>后续暂无交汇';
                        const targetText = line.mode === 'neckline' && line.head ? '<br>头部: ' + line.head.date + ' @' + line.head.price.toFixed(2) + '<br>量度跌幅: ' + line.head.measure.toFixed(2) + (line.targetPrice !== null ? '<br>目标价: ' + line.targetPrice.toFixed(2) : '') : '';
                        return (line.mode === 'neckline' ? '手画颈线' : '手动画线') + line.id + '<br>起点: ' + line.start.date + ' @' + line.start.price.toFixed(2) + '<br>终点: ' + line.end.date + ' @' + line.end.price.toFixed(2) + '<br>角度: ' + line.angle.toFixed(1) + '°<br>日斜率: ' + line.slope.toFixed(3) + crossText + targetText;
                    }}
                }},
                markPoint: {{ data: markData }}
            }};
        }}

        function makeManualSeries() {{
            return manualLines.map(manualLineToSeries);
        }}

        function refreshManualSeries() {{
            chart.setOption({{ series: [...baseSeries, ...makeManualSeries()] }}, {{ replaceMerge: ['series'] }});
        }}

        function startManualDrag(event) {{
            if (!manualDrawingEnabled) {{
                return;
            }}
            const point = pixelToManualPoint(event);
            if (!point) {{
                return;
            }}
            manualDragStart = point;
            manualDraftLine = null;
            setManualPreviewGraphic(null);
            event.event && event.event.preventDefault && event.event.preventDefault();
        }}

        function updateManualDrag(event) {{
            if (!manualDrawingEnabled || !manualDragStart) {{
                return;
            }}
            const point = pixelToManualPoint(event);
            if (!point) {{
                return;
            }}
            manualDraftLine = buildManualLine(manualDragStart, point, true);
            setManualPreviewGraphic(manualDraftLine);
            formatManualStatus();
        }}

        function finishManualDrag(event) {{
            if (!manualDrawingEnabled || !manualDragStart) {{
                return;
            }}
            const point = pixelToManualPoint(event);
            const line = buildManualLine(manualDragStart, point, false);
            if (line) {{
                manualLines.push(line);
                selectedManualLineId = line.id;
            }}
            manualDragStart = null;
            manualDraftLine = null;
            setManualPreviewGraphic(null);
            updateManualButtons();
            refreshManualSeries();
        }}

        manualDrawBtn.addEventListener('click', () => {{
            manualDrawingEnabled = !manualDrawingEnabled;
            manualDragStart = null;
            manualDraftLine = null;
            selectedManualLineId = null;
            setManualPreviewGraphic(null);
            updateManualButtons();
            refreshManualSeries();
        }});
        manualLineMode.addEventListener('change', updateManualButtons);
        chart.on('click', function(params) {{
            if (params.componentType === 'series' && (params.seriesName === '手动画线' || params.seriesName === '手画颈线')) {{
                const deleteId = params.data && params.data.manualDeleteLineId ? params.data.manualDeleteLineId : null;
                if (deleteId !== null) {{
                    manualLines = manualLines.filter(line => line.id !== deleteId);
                    selectedManualLineId = null;
                }} else {{
                    selectedManualLineId = params.seriesOption && params.seriesOption.manualLineId ? params.seriesOption.manualLineId : null;
                }}
                updateManualButtons();
                refreshManualSeries();
            }}
        }});
        chart.getZr().on('mousedown', startManualDrag);
        chart.getZr().on('mousemove', updateManualDrag);
        chart.getZr().on('mouseup', finishManualDrag);
        chart.getZr().on('globalout', () => {{
            manualDragStart = null;
            manualDraftLine = null;
            setManualPreviewGraphic(null);
            updateManualButtons();
        }});
        option.series = [...baseSeries, ...makeManualSeries()];
        chart.setOption(option);
        updateManualButtons();
        window.addEventListener('resize', () => {{
            chart.resize();
            setManualPreviewGraphic(manualDraftLine);
        }});
    </script>
</body>
</html>'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'图表已生成: {output_path}')
    print(f'文件大小: {len(html)/1024:.1f} KB')
    print(f'交易日: {len(dates)}, 买入: {len(buy_points)}, 卖出: {len(sell_points)}')
    print(f'市场状态段数: {len(state_areas)}')
    print(f'上升趋势低点数: {len(lows)}, 上升趋势线数: {len(trend_lines)}')
    print(f'下降趋势高点数: {len(highs)}, 下降趋势线数: {len(down_trend_lines)}')
    print(f'头肩顶颈线数: {len(head_shoulders)}')

def main():
    """主函数"""
    auto_open = '-o' in sys.argv or '--open' in sys.argv
    
    annotate_output_file_with_trend_breaks('out_put.txt')
    dates, prices, market_states, buy_points, sell_points = parse_output_file('out_put.txt')
    output_path = 'stock_chart_with_states.html'
    generate_standalone_html(dates, prices, market_states, buy_points, sell_points, output_path)
    
    if auto_open:
        html_file = os.path.abspath(output_path)
        if os.path.exists(html_file):
            print(f'[正在打开图表: {html_file}]')
            if os.name == 'nt':
                os.startfile(html_file)
            elif os.name == 'posix':
                import subprocess
                subprocess.run(['open', html_file])
    
    return output_path

if __name__ == '__main__':
    main()
