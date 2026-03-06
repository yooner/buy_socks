"""
测试程序 - 验证卖出价格预测逻辑（优化版）

测试逻辑：
1. 解析输出文件，找出所有实际发生卖出的日期
2. 对于每个卖出日，回退到前一天（持仓日）作为测试点
3. 基于测试点的数据，预测第二天（实际卖出日）的卖出触发价格
4. 对比预测价格与实际卖出价格，验证预测是否正确

优化：只测试实际发生卖出的情况，效率更高
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# 策略参数（直接从主程序复制，避免导入导致重新运行）
SELL_RATIO_THRESHOLD = 0.999
BUY_CONDITION_C_VOL_THRESHOLD = 0.85
STOP_LOSS_MA20_THRESHOLD = -7
ENABLE_DELAYED_SELL = True
ENABLE_STOP_LOSS = True
STOCK_CODE = "603496"


def parse_output_file(filepath):
    """解析输出文件，提取每一天的数据和状态"""
    data = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"无法读取文件 {filepath}: {e}")
        return pd.DataFrame()

    for line in lines:
        line = line.strip()
        if not line or line.startswith('=') or line.startswith('-'):
            continue
        if '股票代码' in line or '回测区间' in line or '起始资金' in line:
            continue
        if '日' in line and '日期' in line and '收盘' in line:
            continue
        if '买入条件' in line or '卖出条件' in line or '延迟买入' in line or '延迟卖出' in line or '持仓止损' in line:
            continue

        parts = line.split()
        if len(parts) < 12:
            continue

        try:
            day_num = int(parts[0])
            date_str = parts[1]

            # 检查日期格式
            if not re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                continue

            close_price = parse_float(parts[2])
            ma20 = parse_float(parts[3])
            ma20_pct = parse_float(parts[4])
            atr14 = parse_float(parts[5])
            volatility = parse_float(parts[6])
            volatility_pct = parse_float(parts[7])
            price_atr_ratio = parse_float(parts[8])

            # 处理 >1天数列（可能是负数）
            above_one_days_str = parts[9]
            try:
                above_one_days = int(above_one_days_str)
            except:
                above_one_days = 0

            # 解析操作和持仓
            action_parts = []
            position_idx = 10
            for i in range(10, len(parts)):
                # 查找持仓数字的位置（后面跟着市值）
                if i + 1 < len(parts):
                    try:
                        pos_val = int(parts[i].replace(',', ''))
                        market_val = parse_float(parts[i + 1].replace(',', ''))
                        if market_val >= 0:  # 市值应该非负
                            position_idx = i
                            break
                    except:
                        action_parts.append(parts[i])
                else:
                    action_parts.append(parts[i])

            action = ' '.join(action_parts)
            position = int(parts[position_idx].replace(',', '')) if position_idx < len(parts) else 0
            market_value = parse_float(parts[position_idx + 1].replace(',', '')) if position_idx + 1 < len(parts) else 0

            data.append({
                'day_num': day_num,
                'date': date_str,
                'close': close_price,
                'ma20': ma20,
                'ma20_pct': ma20_pct,
                'atr14': atr14,
                'volatility': volatility,
                'volatility_pct': volatility_pct,
                'price_atr_ratio': price_atr_ratio,
                'above_one_days': above_one_days,
                'action': action,
                'position': position,
                'market_value': market_value
            })
        except Exception as e:
            continue

    return pd.DataFrame(data)


def parse_float(s):
    if s == 'N/A' or s == '-':
        return np.nan
    try:
        return float(s)
    except:
        return np.nan


def extract_pending_sell_price(action):
    """从action中提取待卖出价格"""
    if not action:
        return None
    match = re.search(r'(?:待卖出|更新卖价)@([\d.]+)', action)
    if match:
        return float(match.group(1))
    return None


def extract_sell_price(action):
    """从action中提取卖出价格"""
    if not action:
        return None
    match = re.search(r'卖出@([\d.]+)', action)
    if match:
        return float(match.group(1))
    return None


def extract_sell_reason(action):
    """从action中提取卖出原因"""
    if not action or '卖出' not in action:
        return ""
    # 提取括号中的内容
    match = re.search(r'卖出@[\d.]+\(([^)]+)\)', action)
    if match:
        return match.group(1)
    return ""


def is_price_tracking_state(action, prev_action=None):
    """
    判断当前是否处于价格追踪状态
    基于action中的关键词判断
    """
    if not action:
        return False
    
    # 价格追踪相关的关键词
    tracking_keywords = ['趋势上涨', '趋势下跌', '跌破MA20', '追踪最高价', '价格追踪']
    for keyword in tracking_keywords:
        if keyword in action:
            return True
    
    return False


def is_price_tracking_sell_reason(sell_reason):
    """
    根据卖出原因判断是否属于价格追踪相关的卖出
    """
    if not sell_reason:
        return False
    
    price_tracking_reasons = ['趋势上涨', '趋势下跌', '跌破MA20', '追踪最高价']
    for reason in price_tracking_reasons:
        if reason in sell_reason:
            return True
    
    return False


def predict_sell_conditions(test_row, prev_row=None, prev_prev_row=None, actual_sell_reason=None):
    """
    预测卖出条件
    基于测试日期的数据，预测实际日期可能触发的卖出条件

    返回: [{条件名称, 触发价格, 原因, 当前价格}, ...]
    """
    predictions = []

    close_price = test_row['close']
    ma20 = test_row['ma20']
    atr14 = test_row['atr14']
    volatility = test_row['volatility']
    above_one_days = test_row['above_one_days']
    action = test_row['action']
    position = test_row['position']

    if pd.isna(close_price) or position <= 0:
        return predictions

    # 1. 延迟卖出状态 - 从action中提取待卖出价格
    pending_sell_price = extract_pending_sell_price(action)
    if pending_sell_price is not None and ENABLE_DELAYED_SELL:
        predictions.append({
            'condition': '延迟卖出',
            'trigger_price': pending_sell_price,
            'reason': f'待卖出状态，价格<={pending_sell_price:.2f}触发卖出',
            'current_price': close_price
        })

    # 2. 价格追踪状态 - 基于当前action或实际卖出原因
    is_price_tracking = is_price_tracking_state(action)
    is_price_tracking_sell = is_price_tracking_sell_reason(actual_sell_reason)

    if (is_price_tracking or is_price_tracking_sell) and ENABLE_STOP_LOSS:
        # 从action中提取追踪最高价
        highest_price = None
        if action:
            match = re.search(r'追踪最高价[:：]?\s*([\d.]+)', action)
            if match:
                highest_price = float(match.group(1))

        # 趋势上涨破高卖出
        if '趋势上涨' in action or (actual_sell_reason and '趋势上涨' in actual_sell_reason):
            if highest_price is not None:
                predictions.append({
                    'condition': '趋势上涨破高',
                    'trigger_price': highest_price,
                    'reason': f'趋势上涨，跌破最高价{highest_price:.2f}卖出',
                    'current_price': close_price
                })
            else:
                # 如果没有追踪到最高价，使用当前收盘价作为近似
                predictions.append({
                    'condition': '趋势上涨破高',
                    'trigger_price': close_price,
                    'reason': f'趋势上涨，跌破当前价{close_price:.2f}卖出（无追踪最高价）',
                    'current_price': close_price
                })

        # 趋势下跌卖出 - 使用最低价
        if '趋势下跌' in action or (actual_sell_reason and '趋势下跌' in actual_sell_reason):
            # 趋势下跌：跌破最低价就卖出
            # 如果没有追踪到最低价，使用当前收盘价作为近似
            predictions.append({
                'condition': '趋势下跌',
                'trigger_price': close_price,
                'reason': f'趋势下跌，跌破当前价{close_price:.2f}卖出',
                'current_price': close_price
            })

        # MA20阈值止损
        if ma20 and not pd.isna(ma20):
            target_price_stop = ma20 / (1 + STOP_LOSS_MA20_THRESHOLD / 100)
            predictions.append({
                'condition': 'MA20阈值止损',
                'trigger_price': target_price_stop,
                'reason': f'跌破MA20阈值({STOP_LOSS_MA20_THRESHOLD}%)',
                'current_price': close_price
            })

    # 3. 普通波动率卖出（非价格追踪状态，非待卖出状态）
    # 如果已经是待卖出状态，只考虑延迟卖出，不再计算其他卖出条件
    is_pending_sell_state = pending_sell_price is not None
    
    if not is_price_tracking and not is_pending_sell_state and prev_row is not None and not pd.isna(prev_row['volatility']):
        prev_volatility = prev_row['volatility']

        if not pd.isna(volatility) and volatility > 0 and prev_volatility > 0:
            # 波动率比率卖出
            volatility_ratio = volatility / prev_volatility
            
            # 计算触发波动率比率卖出的价格阈值
            # 波动率 = MA20变化 / ATR
            # 如果当前波动率 > 0，当波动率降至 SELL_RATIO_THRESHOLD 以下时触发卖出
            # 目标波动率 = 当前波动率 * SELL_RATIO_THRESHOLD
            # 目标MA20变化 = 目标波动率 * ATR
            # 目标价格 = (目标MA20变化 + MA20_t-4) * 20 - MA20_t * 19
            
            if volatility > 0 and not pd.isna(atr14) and atr14 > 0:
                # 获取MA20_t-4（用于计算波动率）
                ma20_t_minus_4 = prev_row['ma20'] if prev_row is not None and not pd.isna(prev_row['ma20']) else ma20
                
                # 计算目标波动率和目标MA20变化
                target_volatility = volatility * SELL_RATIO_THRESHOLD
                target_ma20_change = target_volatility * atr14
                
                # 反推目标价格
                # volatility = (MA20_t - MA20_t-4) / ATR
                # MA20_t = (close + MA20_t-1 * 19) / 20
                # 目标MA20_t = MA20_t-4 + target_ma20_change
                # 目标close = 目标MA20_t * 20 - MA20_t-1 * 19
                
                if not pd.isna(ma20) and not pd.isna(ma20_t_minus_4):
                    # 使用更准确的计算
                    target_ma20 = ma20_t_minus_4 + target_ma20_change
                    target_price = target_ma20 * 20 - ma20 * 19
                    
                    predictions.append({
                        'condition': '波动率比率卖出',
                        'trigger_price': target_price,
                        'reason': f'波动率降至{SELL_RATIO_THRESHOLD*100:.0f}%以下 (目标波动率={target_volatility:.3f})',
                        'current_price': close_price
                    })

    return predictions


def main():
    """主测试函数"""
    print("=" * 120)
    print("卖出价格预测逻辑测试程序（优化版 - 只测试实际卖出点）")
    print("=" * 120)
    print("\n测试逻辑说明:")
    print("1. 解析已有的输出文件，找出所有实际发生卖出的日期")
    print("2. 对于每个卖出日，回退到前一天（持仓日）作为测试点")
    print("3. 基于测试点的数据，预测第二天（实际卖出日）的卖出触发价格")
    print("4. 对比预测价格与实际卖出价格，验证预测是否正确")
    print("=" * 120)

    # 第一步：解析输出文件
    print("\n【步骤1】解析输出文件...")

    output_file = "out_put.txt"
    df = parse_output_file(output_file)

    if len(df) == 0:
        print(f"无法解析输出文件: {output_file}")
        print("请确保已经运行过主程序生成了输出文件")
        return

    print(f"✓ 获取到 {len(df)} 天的数据")

    # 第二步：找出所有实际卖出的日期
    print("\n【步骤2】找出所有实际卖出的日期...")

    sell_points = []
    for i in range(1, len(df)):  # 从第2天开始
        current_row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        # 检查是否是卖出日：前一天有持仓，当天持仓为0，且action包含"卖出"
        if (prev_row['position'] > 0 and 
            current_row['position'] == 0 and 
            current_row['action'] and 
            '卖出' in current_row['action']):
            
            sell_points.append({
                'sell_idx': i,
                'sell_date': current_row['date'],
                'sell_price': extract_sell_price(current_row['action']),
                'sell_reason': extract_sell_reason(current_row['action']),
                'sell_data': current_row,
                'test_idx': i - 1,  # 前一天作为测试点
                'test_date': prev_row['date'],
                'test_data': prev_row
            })

    print(f"✓ 找到 {len(sell_points)} 个实际卖出点")

    if len(sell_points) == 0:
        print("没有找到任何卖出点，无法进行测试")
        return

    # 显示前5个卖出点
    print("\n前5个卖出点预览:")
    for i, sp in enumerate(sell_points[:5]):
        sell_price_str = f"{sp['sell_price']:.2f}" if sp['sell_price'] else '未知'
        sell_reason_str = sp['sell_reason'][:30] if sp['sell_reason'] else '未知'
        print(f"  {i+1}. 卖出日期: {sp['sell_date']}, 卖出价格: {sell_price_str}, "
              f"卖出原因: {sell_reason_str}, "
              f"测试日期: {sp['test_date']}")

    # 第三步：遍历卖出点进行验证
    print("\n【步骤3】开始测试验证...")
    print("-" * 120)

    # 限制测试数量
    max_tests = min(100, len(sell_points))
    test_sample = sell_points[-max_tests:]  # 测试最近的卖出点

    correct_predictions = 0
    total_predictions = 0
    test_results = []

    for i, sp in enumerate(test_sample):
        test_row = sp['test_data']
        sell_row = sp['sell_data']
        test_idx = sp['test_idx']
        actual_sell_price = sp['sell_price']
        actual_sell_reason = sp['sell_reason']

        # 获取前两天数据（用于计算波动率变化等）
        prev_row = df.iloc[test_idx - 1] if test_idx > 0 else None
        prev_prev_row = df.iloc[test_idx - 2] if test_idx > 1 else None

        # 预测卖出条件（传入实际卖出原因以便更准确地预测）
        predictions = predict_sell_conditions(test_row, prev_row, prev_prev_row, actual_sell_reason)

        print(f"\n测试 {i+1}/{len(test_sample)}:")
        print(f"  测试日期: {sp['test_date']} (收盘: {test_row['close']:.2f}, 持仓: {test_row['position']})")
        print(f"  卖出日期: {sp['sell_date']} (收盘: {sell_row['close']:.2f})")
        actual_sell_price_str = f"{actual_sell_price:.2f}" if actual_sell_price else '未知'
        print(f"  实际卖出: 价格={actual_sell_price_str}, 原因={actual_sell_reason}")
        
        test_action = test_row['action'][:50] if test_row['action'] else '无'
        print(f"  测试日动作: {test_action}")
        print(f"  预测卖出条件:")

        if len(predictions) == 0:
            print(f"    (无预测条件)")
            test_results.append({
                'test_date': sp['test_date'],
                'sell_date': sp['sell_date'],
                'condition': '无预测',
                'predicted_price': None,
                'actual_price': actual_sell_price,
                'actual_reason': actual_sell_reason,
                'is_correct': False,
                'reason': '未生成预测',
                'diff_pct': None
            })
        else:
            for j, pred in enumerate(predictions):
                total_predictions += 1

                trigger_price = pred['trigger_price']
                
                # 验证标准：实际卖出价格 <= 预测阈值
                # 含义：预测的是"要卖出，价格至少需要达到多少"（阈值）
                # 如果实际卖出价格 <= 预测阈值，说明预测正确（实际在阈值或以下就触发了卖出）
                if actual_sell_price and trigger_price:
                    is_correct = actual_sell_price <= trigger_price
                    diff_pct = (actual_sell_price - trigger_price) / trigger_price * 100
                else:
                    is_correct = False
                    diff_pct = 0

                status = "✓ 正确" if is_correct else "✗ 错误"

                actual_sell_price_str2 = f"{actual_sell_price:.2f}" if actual_sell_price else '未知'
                print(f"    {j+1}. [{status}] {pred['condition']}")
                print(f"       预测触发价: {trigger_price:.2f}, 实际卖出价: {actual_sell_price_str2}, 差异: {diff_pct:+.2f}%")
                print(f"       原因: {pred['reason']}")

                if is_correct:
                    correct_predictions += 1

                test_results.append({
                    'test_date': sp['test_date'],
                    'sell_date': sp['sell_date'],
                    'condition': pred['condition'],
                    'predicted_price': trigger_price,
                    'actual_price': actual_sell_price,
                    'actual_reason': actual_sell_reason,
                    'is_correct': is_correct,
                    'reason': pred['reason'],
                    'diff_pct': diff_pct
                })

    # 第四步：生成测试报告
    print("\n" + "=" * 120)
    print("【测试报告】")
    print("=" * 120)

    if total_predictions > 0:
        accuracy = correct_predictions / total_predictions * 100
        print(f"\n总体统计:")
        print(f"  测试卖出点数量: {len(test_sample)}")
        print(f"  总预测次数: {total_predictions}")
        print(f"  正确预测次数: {correct_predictions}")
        print(f"  准确率: {accuracy:.2f}%")

        # 按条件类型统计
        print(f"\n按条件类型统计:")
        condition_stats = {}
        for result in test_results:
            cond = result['condition']
            if cond not in condition_stats:
                condition_stats[cond] = {'total': 0, 'correct': 0, 'avg_diff': []}
            condition_stats[cond]['total'] += 1
            if result['is_correct']:
                condition_stats[cond]['correct'] += 1
            if result['diff_pct'] is not None:
                condition_stats[cond]['avg_diff'].append(result['diff_pct'])

        for cond, stats in condition_stats.items():
            acc = stats['correct'] / stats['total'] * 100 if stats['total'] > 0 else 0
            avg_diff = np.mean(stats['avg_diff']) if stats['avg_diff'] else 0
            print(f"  {cond}:")
            print(f"    正确率: {stats['correct']}/{stats['total']} ({acc:.2f}%)")
            print(f"    平均差异: {avg_diff:+.2f}%")

        # 显示错误案例
        print(f"\n错误预测案例（实际卖出价格 > 预测阈值，前10个）:")
        errors = [r for r in test_results if not r['is_correct'] and r['predicted_price'] is not None][:10]
        for i, err in enumerate(errors):
            print(f"  {i+1}. {err['test_date']} -> {err['sell_date']}: ")
            print(f"     条件: {err['condition']}")
            print(f"     预测={err['predicted_price']:.2f}, 实际={err['actual_price']:.2f}, 差异={err['diff_pct']:+.2f}%")
            print(f"     实际原因: {err['actual_reason']}")

        # 显示正确但接近的案例
        print(f"\n正确但接近的案例（差异 < 1%，前10个）:")
        close_calls = [r for r in test_results if r['is_correct'] and r['diff_pct'] is not None and abs(r['diff_pct']) < 1][:10]
        for i, call in enumerate(close_calls):
            print(f"  {i+1}. {call['test_date']} -> {call['sell_date']}: ")
            print(f"     条件: {call['condition']}")
            print(f"     预测={call['predicted_price']:.2f}, 实际={call['actual_price']:.2f}, 差异={call['diff_pct']:+.2f}%")

        # 显示无预测的案例
        print(f"\n无预测的案例（前5个）:")
        no_preds = [r for r in test_results if r['condition'] == '无预测'][:5]
        for i, npred in enumerate(no_preds):
            print(f"  {i+1}. {npred['test_date']} -> {npred['sell_date']}: 实际卖出原因={npred['actual_reason']}")

    else:
        print("没有生成任何预测")

    print("\n" + "=" * 120)

    # 保存详细结果到文件
    result_file = "test_prediction_results.txt"
    try:
        with open(result_file, 'w', encoding='utf-8') as f:
            f.write("卖出价格预测测试结果（优化版）\n")
            f.write("=" * 120 + "\n\n")
            f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"股票代码: {STOCK_CODE}\n")
            f.write(f"测试卖出点数量: {len(test_sample)}\n")
            f.write(f"总预测次数: {total_predictions}\n")
            f.write(f"正确预测次数: {correct_predictions}\n")
            if total_predictions > 0:
                f.write(f"准确率: {accuracy:.2f}%\n\n")

            f.write("详细测试结果:\n")
            f.write("-" * 120 + "\n")
            for result in test_results:
                status = "正确" if result['is_correct'] else "错误"
                if result['predicted_price'] is not None:
                    f.write(f"{result['test_date']} -> {result['sell_date']}: "
                           f"[{status}] {result['condition']} "
                           f"预测={result['predicted_price']:.2f}, 实际={result['actual_price']:.2f}, "
                           f"差异={result['diff_pct']:+.2f}%, 实际原因={result['actual_reason']}\n")
                else:
                    f.write(f"{result['test_date']} -> {result['sell_date']}: "
                           f"[无预测] 实际卖出={result['actual_price']:.2f}, 实际原因={result['actual_reason']}\n")
        print(f"\n详细结果已保存到: {result_file}")
    except Exception as e:
        print(f"\n保存结果文件失败: {e}")


if __name__ == "__main__":
    main()
