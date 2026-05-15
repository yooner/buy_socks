#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import json
import os
import sys

# 市场状态颜色配置
STATE_COLORS = {
    '上升趋势': '#ff6b6b',      # 红色
    '自然回撤': '#4ecdc4',      # 青色
    '下降趋势': '#45b7d1',      # 蓝色
    '自然回升': '#96ceb4',      # 绿色
    '次级回撤': '#dfe6e9',      # 浅灰
    '次级回升': '#fdcb6e',      # 黄色
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

def generate_standalone_html(dates, prices, market_states, buy_points, sell_points, output_path):
    # 读取echarts库文件
    try:
        with open('echarts.min.js', 'r', encoding='utf-8') as f:
            echarts_code = f.read()
    except:
        print("警告: 未找到echarts.min.js文件，将使用CDN链接")
        echarts_code = None
    
    # 生成市场状态区域
    state_areas = generate_market_state_areas(dates, market_states)
    
    buy_data_json = json.dumps(buy_points, ensure_ascii=False)
    sell_data_json = json.dumps(sell_points, ensure_ascii=False)
    dates_json = json.dumps(dates, ensure_ascii=False)
    prices_json = json.dumps(prices, ensure_ascii=False)
    states_json = json.dumps(market_states, ensure_ascii=False)
    state_areas_json = json.dumps(state_areas, ensure_ascii=False)
    state_colors_json = json.dumps(STATE_COLORS, ensure_ascii=False)
    
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
                    return r;
                }}
            }},
            grid: {{ left: '3%', right: '4%', bottom: '15%', top: '10%', containLabel: true }},
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
            series: [{{
                name: '收盘价',
                type: 'line',
                data: prices,
                smooth: true,
                symbol: 'none',
                lineStyle: {{ color: '#5470c6', width: 2 }},
                areaStyle: {{ color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{{offset: 0, color: 'rgba(84,112,198,0.3)'}}, {{offset: 1, color: 'rgba(84,112,198,0.05)'}}] }} }},
                markPoint: {{ data: [...buyMarks, ...sellMarks], symbol: 'pin', symbolSize: 45, label: {{ fontSize: 10 }} }},
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
            }}]
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
