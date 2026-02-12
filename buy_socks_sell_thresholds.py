import pandas as pd
from typing import Dict, List, Optional, Tuple
from ana_stocks import (
    get_weekly_data, 
    calculate_indicators,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    END_DATE_EXPORT as END_DATE,
    get_year_range
)


INITIAL_CAPITAL = 100000

buy_levels = [-0.04, -0.08, -0.13, -0.19, -0.26]
buy_ratios = [0.10, 0.15, 0.20, 0.25, 0.30]

sell_thresholds = [0.08, 0.12, 0.18]
sell_ratios = [0.30, 0.30, 0.40]


def calculate_ma20(prices: List[float]) -> float:
    if len(prices) < 20:
        return sum(prices) / len(prices) if prices else 0
    return sum(prices[-20:]) / 20


def run_backtest(stock_code: str = STOCK_CODE):
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    weekly_data = get_weekly_data(stock_code, days=365 * BACKTEST_YEARS)

    if len(weekly_data) < 21:
        print(f"数据不足，需要至少21周数据，当前只有{len(weekly_data)}周")
        return None, {}

    left_cash = INITIAL_CAPITAL
    shares = 0
    buy_count = 0
    sell_count = 0
    trade_history = []

    price_history = []

    yearly_summary = {}
    last_year = None
    prev_year_end_asset = INITIAL_CAPITAL

    print(f"\n{'='*80}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"买入档位: {buy_levels}")
    print(f"买入比例: {buy_ratios}")
    print(f"卖出档位: {sell_thresholds}")
    print(f"卖出比例: {sell_ratios}")
    print(f"{'='*80}\n")

    print(f"{'周序号':<6} {'日期':<12} {'收盘价':>10} {'MA20':>10} {'持有股数':>10} {'剩余资金':>12} {'总资产':>12} {'操作':<20}")
    print("-" * 85)

    weekly_data = calculate_indicators(weekly_data)
    price_history = []

    for week_idx in range(len(weekly_data)):
        current_week = week_idx + 1
        row = weekly_data.iloc[week_idx]
        current_price = row['close']
        current_date = row['date'].strftime('%Y-%m-%d')
        current_year = row['date'].year

        price_history.append(current_price)
        ma20 = row['ma20'] if pd.notna(row['ma20']) else calculate_ma20(price_history)

        operation = ""

        if current_week >= 21:
            if shares == 0:
                if buy_count < len(buy_levels):
                    level = buy_levels[buy_count]
                    expected_price = ma20 * (1 + level)
                    if current_price <= expected_price:
                        buy_ratio = buy_ratios[buy_count]
                        buy_amount = left_cash * buy_ratio
                        new_shares = int(buy_amount / current_price)

                        if new_shares > 0:
                            cost = new_shares * current_price
                            left_cash -= cost
                            shares += new_shares
                            buy_count += 1
                            operation = f"买入{buy_ratio*100:.0f}%"
                            trade_history.append({
                                'week': current_week,
                                'date': current_date,
                                'price': current_price,
                                'shares': new_shares,
                                'cost': cost,
                                'total_cost': cost,
                                'type': 'BUY',
                                'buy_count': buy_count
                            })
            else:
                sold_all = False

                if sell_count < len(sell_thresholds):
                    threshold = sell_thresholds[sell_count]
                    if current_price >= ma20 * (1 + threshold):
                        sell_ratio = sell_ratios[sell_count]
                        if sell_count == len(sell_ratios) - 1:
                            sell_shares = shares
                        else:
                            sell_shares = int(shares * sell_ratio)
                        if sell_shares > 0:
                            sell_amount = sell_shares * current_price
                            left_cash += sell_amount
                            shares -= sell_shares
                            sell_count += 1
                            total_asset = left_cash + shares * current_price
                            if shares == 0:
                                operation = f"清仓 总资产{total_asset:.0f}"
                                sold_all = True
                            else:
                                operation = f"减仓{sell_ratio*100:.0f}% 总资产{total_asset:.0f}"

                if sold_all:
                    buy_count = 0
                    sell_count = 0
                elif shares > 0 and buy_count < len(buy_levels):
                    level = buy_levels[buy_count]
                    expected_price = ma20 * (1 + level)
                    if current_price <= expected_price:
                        buy_ratio = buy_ratios[buy_count]
                        buy_amount = left_cash * buy_ratio
                        new_shares = int(buy_amount / current_price)

                        if new_shares > 0:
                            cost = new_shares * current_price
                            left_cash -= cost
                            shares += new_shares
                            buy_count += 1
                            operation = f"再买{buy_ratio*100:.0f}%"
                            trade_history.append({
                                'week': current_week,
                                'date': current_date,
                                'price': current_price,
                                'shares': new_shares,
                                'cost': cost,
                                'total_cost': sum(t['cost'] for t in trade_history if t['type'] == 'BUY') + cost,
                                'type': 'BUY',
                                'buy_count': buy_count
                            })

        total_asset = left_cash + shares * current_price

        print(f"{current_week:<6} {current_date:<12} {current_price:>10.2f} {ma20:>10.2f} {shares:>10} {left_cash:>12.2f} {total_asset:>12.2f} {operation:<20}")

        if last_year is None:
            last_year = current_year
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}

        if current_year != last_year:
            yearly_summary[last_year]['end_asset'] = total_asset
            prev_year_end_asset = total_asset
            yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
            last_year = current_year

        yearly_summary[current_year]['end_asset'] = total_asset

    print(f"\n{'='*80}")
    final_asset = left_cash + shares * weekly_data.iloc[-1]['close']
    print(f"\n{'='*80}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"最终资产: {final_asset:.2f} 元")
    print(f"收益率: {(final_asset / INITIAL_CAPITAL - 1) * 100:.2f}%")
    print(f"{'='*80}\n")

    print(f"{'='*60}")
    print(f"年度收益:")
    print(f"{'='*60}")
    sorted_years = sorted(yearly_summary.keys())
    yearly_returns = {}
    for year in sorted_years:
        if 'end_asset' in yearly_summary[year]:
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
