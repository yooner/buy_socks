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
    'min_decline_pct': 5.0,       # 低点距离高点下跌最小百分比（可配置）
    'min_rebound_pct': 5.0,       # 低点之后反弹最小百分比（可配置，默认7%）
    'min_slope_angle': 35,        # 趋势线最小斜率角度（可配置，默认30度）
    'max_slope_angle': 60,        # 趋势线最大斜率角度（可配置，默认80度）
    'price_scale': 2.0,           # 价格缩放因子，用于调整角度敏感度
    'allow_negative_slope': False, # 是否允许负斜率（默认不允许）
}

def parse_output_file(filepath):
    dates = []
    prices = []
    market_states = []  # 每天的市场状态
    buy_points = []
    sell_points = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
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
            
            if '买入@' in line:
                match = re.search(r'买入@([\d.]+)\(([^)]+)\)', line)
                if match:
                    buy_points.append({
                        'date': date_str,
                        'price': float(match.group(1)),
                        'type': match.group(2)
                    })
            
            if '卖出@' in line:
                match = re.search(r'卖出@([\d.]+)\s+盈亏:([+-]?\d+)\(([+-]?[\d.]+)%\)', line)
                if match:
                    sell_points.append({
                        'date': date_str,
                        'price': float(match.group(1)),
                        'profit': match.group(2),
                        'profit_pct': match.group(3)
                    })
        except:
            continue
    
    return dates, prices, market_states, buy_points, sell_points

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


def find_uptrend_lows(dates, prices, market_states, config):
    """
    识别上升趋势和自然回撤中的低点（延迟判定版本）
    
    低点定义：高点 -> 低点（下跌超过7%）-> 反弹超过7% 形成一个低点
    
    特点：
    1. 低点是相对于前面的高点，不是前一天
    2. 延迟判定 - 等价格涨上去后再回头确认是否是低点
    3. 持续更新 - 如果价格继续走低，更新低点位置
    
    返回：低点列表 [(日期索引, 日期, 价格), ...]
    """
    lows = []
    min_decline_pct = config['min_decline_pct']
    min_rebound_pct = config.get('min_rebound_pct', 7.0)
    
    # 可以画线的状态：上升趋势和自然回撤
    valid_states = ['上升趋势', '自然回撤']
    
    # 按连续的上升趋势+自然回撤段来处理
    i = 0
    while i < len(prices):
        # 跳过非有效状态
        if market_states[i] not in valid_states:
            i += 1
            continue
        
        # 找到连续的上升趋势+自然回撤段
        segment_start = i
        while i < len(prices) and market_states[i] in valid_states:
            i += 1
        segment_end = i - 1
        
        # 处理这个连续段，使用延迟判定找低点
        segment_prices = prices[segment_start:segment_end+1]
        segment_dates = dates[segment_start:segment_end+1]
        segment_states = market_states[segment_start:segment_end+1]
        
        # 使用栈的方式处理：寻找下跌超过阈值后反弹超过阈值的点
        idx = 0
        while idx < len(segment_prices):
            # 找当前段的最高点作为参考
            high_price = segment_prices[idx]
            high_idx = idx
            
            # 向前找最高点
            for k in range(idx, len(segment_prices)):
                if segment_prices[k] > high_price:
                    high_price = segment_prices[k]
                    high_idx = k
                # 如果开始下跌超过阈值，停止找高点
                elif (high_price - segment_prices[k]) / high_price * 100 >= min_decline_pct:
                    break
            
            # 从高点开始找低点
            current_low_price = high_price
            current_low_idx = high_idx
            
            # 持续更新低点，直到开始反弹
            k = high_idx + 1
            while k < len(segment_prices):
                price = segment_prices[k]
                
                # 如果价格创新低，更新低点
                if price < current_low_price:
                    current_low_price = price
                    current_low_idx = k
                
                # 计算从高点到当前低点的跌幅
                decline_pct = (high_price - current_low_price) / high_price * 100
                
                # 如果跌幅超过阈值，检查是否开始反弹
                if decline_pct >= min_decline_pct:
                    # 计算从低点反弹的幅度
                    rebound_pct = (price - current_low_price) / current_low_price * 100
                    
                    # 如果反弹超过阈值，确认低点
                    if rebound_pct >= min_rebound_pct:
                        # 确认找到低点
                        lows.append({
                            'index': segment_start + current_low_idx,
                            'date': segment_dates[current_low_idx],
                            'price': current_low_price,
                            'decline_pct': decline_pct,
                            'rebound_pct': rebound_pct,
                            'state': segment_states[current_low_idx]
                        })
                        # 从当前位置继续找下一个低点
                        idx = k
                        break
                
                k += 1
            
            # 如果走到末尾还没找到反弹，退出
            if k >= len(segment_prices):
                break
            
            idx += 1
    
    return lows


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


def generate_trend_lines(lows, dates, prices, market_states, config):
    """
    生成趋势线
    
    连接两个低点，要求：
    1. 斜率在 min_slope_angle 和 max_slope_angle 之间（正斜率）
    2. 两个低点必须在同一个连续的上升趋势+自然回撤段内
    3. 不跨越其他市场状态
    """
    trend_lines = []
    min_angle = config['min_slope_angle']
    max_angle = config['max_slope_angle']
    price_scale = config.get('price_scale', 2.0)
    allow_negative = config.get('allow_negative_slope', False)
    
    valid_states = ['上升趋势', '自然回撤']
    
    if len(lows) < 2:
        return trend_lines
    
    # 按索引排序低点
    sorted_lows = sorted(lows, key=lambda x: x['index'])
    
    # 用于跟踪哪些低点已经被用作趋势线的起点或终点
    used_low_indices = set()
    
    for i in range(len(sorted_lows) - 1):
        if i in used_low_indices:
            continue
            
        low1 = sorted_lows[i]
        best_line = None
        best_angle_diff = float('inf')
        best_j = -1
        
        # 寻找与当前低点形成最佳趋势线的下一个低点
        for j in range(i + 1, len(sorted_lows)):
            low2 = sorted_lows[j]
            
            # 检查两个低点之间是否都是上升趋势或自然回撤（不能跨越其他状态）
            start_idx = low1['index']
            end_idx = low2['index']
            
            # 验证这段区间内是否都是有效状态
            is_continuous = True
            for k in range(start_idx, end_idx + 1):
                if k < len(market_states) and market_states[k] not in valid_states:
                    is_continuous = False
                    break
            
            if not is_continuous:
                continue
            
            days_diff = end_idx - start_idx
            if days_diff < 5:  # 至少间隔5天
                continue
            
            # 计算斜率角度
            angle = calculate_slope_angle(low1['price'], low2['price'], days_diff, price_scale)
            
            # 检查斜率条件
            # 如果不允许负斜率，则只接受正角度
            if not allow_negative and angle < 0:
                continue
            
            # 检查角度是否在范围内
            if min_angle <= abs(angle) <= max_angle:
                # 计算与45度的偏差（偏好接近45度的线）
                angle_diff = abs(abs(angle) - 45)
                if angle_diff < best_angle_diff:
                    best_angle_diff = angle_diff
                    best_line = {
                        'start': low1,
                        'end': low2,
                        'angle': angle
                    }
                    best_j = j
        
        # 如果找到符合条件的趋势线，添加到结果
        if best_line:
            trend_lines.append(best_line)
            used_low_indices.add(i)
            used_low_indices.add(best_j)
    
    return trend_lines

def generate_standalone_html(dates, prices, market_states, buy_points, sell_points, output_path, lows=None, trend_lines=None):
    # 读取echarts库文件
    try:
        with open('echarts.min.js', 'r', encoding='utf-8') as f:
            echarts_code = f.read()
    except:
        print("警告: 未找到echarts.min.js文件，将使用CDN链接")
        echarts_code = None
    
    # 生成市场状态区域
    state_areas = generate_market_state_areas(dates, market_states)
    
    # 识别上升趋势低点和趋势线
    if lows is None:
        lows = find_uptrend_lows(dates, prices, market_states, TREND_LINE_CONFIG)
    if trend_lines is None:
        trend_lines = generate_trend_lines(lows, dates, prices, market_states, TREND_LINE_CONFIG)
    
    buy_data_json = json.dumps(buy_points, ensure_ascii=False)
    sell_data_json = json.dumps(sell_points, ensure_ascii=False)
    dates_json = json.dumps(dates, ensure_ascii=False)
    prices_json = json.dumps(prices, ensure_ascii=False)
    states_json = json.dumps(market_states, ensure_ascii=False)
    state_areas_json = json.dumps(state_areas, ensure_ascii=False)
    state_colors_json = json.dumps(STATE_COLORS, ensure_ascii=False)
    lows_json = json.dumps(lows, ensure_ascii=False)
    trend_lines_json = json.dumps(trend_lines, ensure_ascii=False)
    
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
        
        // 生成趋势线数据（延长显示）
        const trendLineSeries = trendLines.map((line, idx) => {{
            // 计算延长线的终点（向后延长100%的时间，即延长一倍）
            const startIdx = dates.indexOf(line.start.date);
            const endIdx = dates.indexOf(line.end.date);
            const daysDiff = endIdx - startIdx;
            const extendDays = Math.floor(daysDiff * 2.0); // 延长200%（两倍）
            const extendedIdx = Math.min(endIdx + extendDays, dates.length - 1);
            
            // 计算延长后的价格（根据斜率）
            const priceDiff = line.end.price - line.start.price;
            const dailySlope = priceDiff / daysDiff;
            const extendedPrice = line.end.price + dailySlope * extendDays;
            
            const extendedDate = dates[extendedIdx];
            
            return {{
                name: '趋势线' + (idx + 1),
                type: 'line',
                data: [
                    [line.start.date, line.start.price],
                    [line.end.date, line.end.price],
                    [extendedDate, extendedPrice]
                ],
                lineStyle: {{ color: '#ff6348', width: 2, type: 'dashed' }},
                symbol: ['none', 'none', 'arrow'], // 尾端显示箭头
                symbolSize: 8,
                silent: true,
                tooltip: {{ show: false }},
                markPoint: {{
                    data: [{{
                        coord: [extendedDate, extendedPrice],
                        value: line.angle.toFixed(1) + '°',
                        itemStyle: {{ color: '#ff6348' }},
                        label: {{ 
                            show: true, 
                            formatter: line.angle.toFixed(1) + '°', 
                            color: '#ff6348', 
                            fontSize: 10, 
                            fontWeight: 'bold',
                            position: 'top'
                        }},
                        symbol: 'circle',
                        symbolSize: 1
                    }}]
                }}
            }};
        }});
        
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
                    // 检查是否是低点
                    const low = uptrendLows.find(l => l.date === d);
                    if (low) {{
                        r += '<br><span style="color:#ffa502;font-weight:bold;">上升趋势低点 @' + low.price.toFixed(2) + '<br>跌幅: ' + low.decline_pct.toFixed(1) + '%</span>';
                    }}
                    return r;
                }}
            }},
            legend: {{
                data: ['收盘价', '趋势线'],
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
            series: [
                {{
                    name: '收盘价',
                    type: 'line',
                    data: prices,
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {{ color: '#5470c6', width: 2 }},
                    areaStyle: {{ color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{{offset: 0, color: 'rgba(84,112,198,0.3)'}}, {{offset: 1, color: 'rgba(84,112,198,0.05)'}}] }} }},
                    markPoint: {{ data: [...buyMarks, ...sellMarks, ...lowMarks], symbol: 'pin', symbolSize: 45, label: {{ fontSize: 10 }} }},
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
                ...trendLineSeries
            ]
        }};
        chart.setOption(option);
        window.addEventListener('resize', () => chart.resize());
    </script>
</body>
</html>'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'图表已生成: {output_path}')
    print(f'文件大小: {len(html)/1024:.1f} KB')
    print(f'交易日: {len(dates)}, 买入: {len(buy_points)}, 卖出: {len(sell_points)}')
    print(f'市场状态段数: {len(state_areas)}')
    print(f'上升趋势低点数: {len(lows)}, 趋势线数: {len(trend_lines)}')

def main():
    """主函数"""
    auto_open = '-o' in sys.argv or '--open' in sys.argv
    
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
