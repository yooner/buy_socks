#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import json
import os
import sys

def parse_output_file(filepath):
    dates = []
    prices = []
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
            
            dates.append(date_str)
            prices.append(close_price)
            
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
    
    return dates, prices, buy_points, sell_points

def generate_standalone_html(dates, prices, buy_points, sell_points, output_path):
    # 读取echarts库文件
    try:
        with open('echarts.min.js', 'r', encoding='utf-8') as f:
            echarts_code = f.read()
    except:
        print("警告: 未找到echarts.min.js文件，将使用CDN链接")
        echarts_code = None
    
    buy_data_json = json.dumps(buy_points, ensure_ascii=False)
    sell_data_json = json.dumps(sell_points, ensure_ascii=False)
    dates_json = json.dumps(dates, ensure_ascii=False)
    prices_json = json.dumps(prices, ensure_ascii=False)
    
    # 如果echarts代码存在，内嵌它；否则使用CDN
    if echarts_code:
        echarts_script = f'<script>{echarts_code}</script>'
    else:
        echarts_script = '<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>'
    
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
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .stats {{ display: flex; justify-content: center; gap: 40px; margin-top: 20px; flex-wrap: wrap; }}
        .stat-item {{ text-align: center; }}
        .stat-value {{ font-size: 24px; font-weight: bold; }}
        .stat-label {{ font-size: 12px; opacity: 0.8; }}
        .legend {{ display: flex; justify-content: center; gap: 40px; padding: 20px; background: #f8f9fa; border-bottom: 1px solid #e9ecef; }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; font-size: 14px; color: #495057; }}
        .legend-dot {{ width: 14px; height: 14px; border-radius: 50%; }}
        .buy-dot {{ background: #ff4757; box-shadow: 0 0 10px rgba(255,71,87,0.5); }}
        .sell-dot {{ background: #2ed573; box-shadow: 0 0 10px rgba(46,213,115,0.5); }}
        #chart {{ width: 100%; height: 600px; padding: 20px; }}
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
        <div id="chart"></div>
    </div>
    <script>
        const dates = {dates_json};
        const prices = {prices_json};
        const buyPoints = {buy_data_json};
        const sellPoints = {sell_data_json};
        
        const buyMarks = buyPoints.map(p => ({{
            coord: [p.date, p.price],
            value: '买',
            itemStyle: {{ color: '#ff4757' }},
            label: {{ show: true, formatter: '买', color: '#fff', fontSize: 11, fontWeight: 'bold' }},
            info: '买入\\n价格: ' + p.price + '\\n类型: ' + p.type
        }}));
        
        const sellMarks = sellPoints.map(p => ({{
            coord: [p.date, p.price],
            value: '卖',
            itemStyle: {{ color: '#2ed573' }},
            label: {{ show: true, formatter: '卖', color: '#fff', fontSize: 11, fontWeight: 'bold' }},
            info: '卖出\\n价格: ' + p.price + '\\n盈亏: ' + p.profit + ' (' + p.profit_pct + '%)'
        }}));
        
        const chart = echarts.init(document.getElementById('chart'));
        const option = {{
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    let r = '日期: ' + params[0].axisValue + '<br>收盘价: ' + params[0].data;
                    const d = params[0].axisValue;
                    buyPoints.forEach(p => {{ if(p.date === d) r += '<br><span style="color:#ff4757;font-weight:bold;">买入 @' + p.price + '<br>类型: ' + p.type + '</span>'; }});
                    sellPoints.forEach(p => {{ if(p.date === d) r += '<br><span style="color:#2ed573;font-weight:bold;">卖出 @' + p.price + '<br>盈亏: ' + p.profit + ' (' + p.profit_pct + '%)</span>'; }});
                    return r;
                }}
            }},
            grid: {{ left: '3%', right: '4%', bottom: '12%', top: '8%', containLabel: true }},
            xAxis: {{ type: 'category', data: dates, boundaryGap: false, axisLabel: {{ rotate: 45, fontSize: 10 }} }},
            yAxis: {{ type: 'value', scale: true, splitLine: {{ lineStyle: {{ type: 'dashed', color: '#eee' }} }} }},
            dataZoom: [{{ type: 'inside', start: 0, end: 100 }}, {{ type: 'slider', start: 0, end: 100, height: 25, bottom: 10 }}],
            series: [{{
                name: '收盘价',
                type: 'line',
                data: prices,
                smooth: true,
                symbol: 'none',
                lineStyle: {{ color: '#5470c6', width: 1.5 }},
                areaStyle: {{ color: {{ type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{{offset: 0, color: 'rgba(84,112,198,0.3)'}}, {{offset: 1, color: 'rgba(84,112,198,0.05)'}}] }} }},
                markPoint: {{ data: [...buyMarks, ...sellMarks], symbol: 'pin', symbolSize: 45, label: {{ fontSize: 10 }} }}
            }}]
        }};
        chart.setOption(option);
        window.addEventListener('resize', () => chart.resize());
    </script>
</body>
</html>'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'独立HTML图表已生成: {output_path}')
    print(f'文件大小: {len(html)/1024:.1f} KB')
    print(f'交易日: {len(dates)}, 买入: {len(buy_points)}, 卖出: {len(sell_points)}')

def main():
    """主函数：生成图表并可选择自动打开"""
    # 检查是否有 -o 参数（自动打开）
    auto_open = '-o' in sys.argv or '--open' in sys.argv
    
    dates, prices, buy_points, sell_points = parse_output_file('out_put.txt')
    output_path = 'stock_chart_standalone.html'
    generate_standalone_html(dates, prices, buy_points, sell_points, output_path)
    
    # 如果指定了 -o 参数，自动打开HTML文件
    if auto_open:
        html_file = os.path.abspath(output_path)
        if os.path.exists(html_file):
            print(f'[正在打开图表: {html_file}]')
            if os.name == 'nt':  # Windows
                os.startfile(html_file)
            elif os.name == 'posix':  # macOS/Linux
                import subprocess
                subprocess.run(['open', html_file])
        else:
            print(f'[警告: 未找到图表文件 {html_file}]')
    
    return output_path

if __name__ == '__main__':
    main()
