"""
趋势买入策略
状态机: 无趋势 → 趋势确认 → 回调等待 → 介入 → 持有 → 趋势延续/失效 → 退出 → 无趋势

卖出策略：基于日线结构性低点
- Step 1: 获取本周日线数据
- Step 2: 日线高点不再创新高 → 开始找日线低点
- Step 3: 日线收盘价跌破这个低点 → 趋势失效，清仓
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from ana_stocks import (
    get_weekly_data,
    get_daily_data,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    INITIAL_CAPITAL_EXPORT as INITIAL_CAPITAL,
    get_year_range
)


def detect_trend(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['ma20'] = df['close'].rolling(20).mean()
    df['slope'] = df['ma20'] - df['ma20'].shift(5)
    df['ma_up'] = df['slope'] > 0
    df['trend_up'] = df['ma_up'].rolling(5).sum() == 5
    df['trend_up'] = df['trend_up'].fillna(False).astype(bool)
    return df


def run_backtest(stock_code: str = STOCK_CODE):
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    weekly_data = get_weekly_data(stock_code, days=365 * BACKTEST_YEARS)

    if weekly_data is None or len(weekly_data) < 25:
        print(f"数据不足，需要至少25周数据，当前只有{len(weekly_data) if weekly_data else 0}周")
        return None, {}

    weekly_data = detect_trend(weekly_data)
    weekly_data['date'] = pd.to_datetime(weekly_data['date'])

    daily_data = get_daily_data(stock_code, days=365 * BACKTEST_YEARS)
    if daily_data is None or len(daily_data) < 60:
        print(f"日线数据不足，需要至少60天数据，当前只有{len(daily_data) if daily_data else 0}天")
        daily_data = pd.DataFrame()
    else:
        daily_data['date'] = pd.to_datetime(daily_data['date'])

    cash = INITIAL_CAPITAL
    shares = 0
    buy_count = 0
    sell_count = 0
    trade_history = []
    completed_trades = []

    structural_low = None
    in_daily_high_broken = False
    last_daily_high = None
    last_daily_close = None

    yearly_summary = {}
    last_year = None
    prev_year_end_asset = INITIAL_CAPITAL

    print(f"\n{'='*80}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"{'='*80}\n")

    header = f"{'周序号':<5} {'日期':<12} {'收盘价':>10} {'MA20':>10} {'日线最高':>10} {'趋势':>6} {'持有股数':>10} {'剩余资金':>12} {'总资产':>12} {'状态':<20} {'操作':<20}"
    print(header)
    print("-" * 150)

    for week_idx in range(len(weekly_data)):
        current_week = week_idx + 1
        row = weekly_data.iloc[week_idx]
        current_price = row['close']
        current_date = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]
        current_year = int(str(current_date)[:4])
        ma20 = row['ma20']
        trend_up = row['trend_up']

        if pd.isna(ma20):
            continue

        if week_idx > 0:
            last_week = weekly_data.iloc[week_idx - 1]
            last_weekly_close = last_week['close']
        else:
            last_weekly_close = current_price

        trend_str = "↑" if trend_up else "↓"
        state_info = ""
        operation = ""
        recent_daily_close = current_price
        recent_daily_high = None

        if daily_data is not None and len(daily_data) > 0:
            week_end = row['date'] + pd.Timedelta(days=7)

            if week_idx == 0:
                week_daily_data = daily_data[daily_data['date'] <= week_end]
            else:
                prev_week_end = weekly_data.iloc[week_idx - 1]['date']
                week_daily_data = daily_data[
                    (daily_data['date'] > prev_week_end) &
                    (daily_data['date'] <= week_end)
                ]

            if len(week_daily_data) > 0:
                recent_daily_high = week_daily_data['最高'].max()
                recent_daily_low = week_daily_data['最低'].min()
                recent_daily_close = week_daily_data['收盘'].iloc[-1]

                # 2. 持仓期间始终监控日线高点失效（无论趋势方向）
                # 注意：先用当前周与上一周的last_daily_high比较，判断当前周是否失效
                # 条件：本周日线最高 < 上周日线最高 AND 本周周线收盘 < 上周周线收盘
                if shares > 0 and not in_daily_high_broken:
                    if (
                        last_daily_high is not None
                        and recent_daily_high < last_daily_high
                        and current_price < last_weekly_close
                    ):
                        in_daily_high_broken = True
                        structural_low = recent_daily_low
                        state_info = f"[日线高点失效@{recent_daily_high:.2f}]"

                # 1. 最后更新上一周的高点和收盘（无论是否持仓）
                last_daily_high = recent_daily_high
                last_daily_close = recent_daily_close

                # 3. 趋势向下或价格偏离MA20超过8%时，开启结构有效性判断
                if shares > 0 and (not trend_up or recent_daily_close < row['ma20'] * 0.92):
                    close_above_ma20 = recent_daily_close > row['ma20']
                    ma20_slope_positive = row['slope'] > 0
                    structure_is_valid = close_above_ma20 and ma20_slope_positive

                    if in_daily_high_broken:
                        if structure_is_valid:
                            if structural_low is None:
                                structural_low = recent_daily_low
                            else:
                                structural_low = max(structural_low, recent_daily_low)
                            in_daily_high_broken = False
                            state_info = f"[日线回调结束@{structural_low:.2f}]"
                        else:
                            state_info = f"[日线冻结{structural_low:.2f}]"

        if current_week >= 25:
            if shares == 0:
                if trend_up and ma20 * 1.05 >= current_price >= ma20:
                    new_shares = int(cash / current_price)
                    if new_shares > 0:
                        cost = new_shares * current_price
                        cash = cash - cost
                        shares = new_shares
                        buy_count += 1
                        operation = f"买入"
                        structural_low = None
                        in_daily_high_broken = False
                        last_daily_high = None
                        last_daily_close = None
                        trade_history.append({
                            'week': current_week,
                            'date': current_date,
                            'price': current_price,
                            'shares': new_shares,
                            'cost': cost,
                            'type': 'BUY'
                        })
            else:
                if in_daily_high_broken and structural_low is not None and recent_daily_close < structural_low:
                    revenue = shares * recent_daily_close
                    cash = revenue
                    profit = revenue - sum(t['cost'] for t in trade_history if t['type'] == 'BUY')
                    sell_count += 1
                    for t in trade_history:
                        if t['type'] == 'BUY':
                            t['sold'] = True
                    operation = f"跌破日线结构低点清仓 盈利{profit:.0f}"
                    sell_trade = {
                        'week': current_week,
                        'date': current_date,
                        'price': recent_daily_close,
                        'shares': shares,
                        'revenue': revenue,
                        'profit': profit,
                        'type': 'SELL'
                    }
                    completed_trades.extend(trade_history)
                    completed_trades.append(sell_trade)
                    trade_history = []
                    shares = 0
                    in_daily_high_broken = False
                    structural_low = None
                    last_daily_high = None
                    last_daily_close = None

        total_asset = cash + shares * recent_daily_close

        daily_high_str = f"{recent_daily_high:>10.2f}" if recent_daily_high is not None else " " * 10
        print(f"{current_week:<5} {current_date:<12} {current_price:>10.2f} {ma20:>10.2f} {daily_high_str} {trend_str:>6} {shares:>10} {cash:>12.2f} {total_asset:>12.2f} {state_info:<20} {operation:<20}")

        if last_year is None:
            last_year = current_year
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}

        if current_year != last_year:
            yearly_summary[last_year]['end_asset'] = total_asset
            prev_year_end_asset = total_asset
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
            last_year = current_year

        yearly_summary[current_year]['end_asset'] = total_asset

    if shares > 0:
        last_close = weekly_data.iloc[-1]['close']
        revenue = shares * last_close
        profit = revenue - sum(t['cost'] for t in trade_history if t['type'] == 'BUY')
        cash = revenue
        sell_count += 1
        for t in trade_history:
            if t['type'] == 'BUY':
                t['sold'] = True
        final_sell_trade = {
            'week': weekly_data.iloc[-1].name + 1,
            'date': str(weekly_data.iloc[-1]['date'])[:10],
            'price': last_close,
            'shares': shares,
            'revenue': revenue,
            'profit': profit,
            'type': 'SELL'
        }
        completed_trades.extend(trade_history)
        completed_trades.append(final_sell_trade)
        print(f"{'最后':<6} {str(weekly_data.iloc[-1]['date'])[:10]:<12} {last_close:>10.2f} {weekly_data.iloc[-1]['ma20']:<10.2f} {'清':>8} {0:>10} {cash:>12.2f} {cash:>12.2f} {'清仓 最终':<20} {'清仓 最终':<30}")

    print(f"\n{'='*80}")
    final_asset = cash
    print(f"\n{'='*80}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"最终资产: {final_asset:.2f} 元")
    print(f"收益率: {(final_asset / INITIAL_CAPITAL - 1) * 100:.2f}%")
    print(f"{'='*80}\n")

    print(f"{'='*60}")
    print(f"买入统计: {buy_count}次")
    print(f"{'='*60}")
    for i, trade in enumerate([t for t in completed_trades if t['type'] == 'BUY'], 1):
        print(f"  第{i}次: {trade['date']} 买入 {trade['shares']}股 @ {trade['price']:.2f}")
    print(f"  总买入次数: {buy_count}")
    print(f"{'='*60}\n")

    print(f"{'='*60}")
    print(f"卖出统计: {sell_count}次")
    print(f"{'='*60}")
    for i, trade in enumerate([t for t in completed_trades if t['type'] == 'SELL'], 1):
        profit_str = f"盈利{trade.get('profit', 0):.0f}" if trade.get('profit', 0) >= 0 else f"亏损{abs(trade.get('profit', 0)):.0f}"
        print(f"  第{i}次: {trade['date']} 卖出 {trade['shares']}股 @ {trade['price']:.2f} ({profit_str})")
    print(f"  总卖出次数: {sell_count}")
    print(f"{'='*60}\n")

    print(f"{'='*60}")
    print("年度收益:")
    print(f"{'='*60}")
    sorted_years = sorted(yearly_summary.keys())
    yearly_returns = {}
    for year in sorted_years:
        if 'end_asset' in yearly_summary[year] and yearly_summary[year]['end_asset'] is not None:
            start = yearly_summary[year]['start_asset']
            end = yearly_summary[year]['end_asset']
            diff = end - start
            pct = (diff / start * 100) if start > 0 else 0
            yearly_returns[year] = pct
            print(f"{year}年: {start:.2f} → {end:.2f} ({diff:+.2f}元, {pct:+.2f}%)")
    print(f"{'='*60}\n")

    total_return = (final_asset / INITIAL_CAPITAL - 1) * 100
    return total_return, yearly_returns


if __name__ == "__main__":
    total_return, yearly_returns = run_backtest()
    print(f"\n汇总结果:")
    print(f"总收益率: {total_return:+.2f}%")
    print(f"年度收益率: {yearly_returns}")
