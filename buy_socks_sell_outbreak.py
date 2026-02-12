"""
趋势爆发策略 - 优化版
- B1: 趋势确认 (50%)
- B2: 首次回踩 (30%)
- B3: 再创新高 (20%)
- 卖出: +30%卖30%, +60%卖30%, 跌破MA20清仓
"""

import pandas as pd
import numpy as np
from ana_stocks import (
    get_weekly_data, 
    calculate_indicators,
    STOCK_CODE_EXPORT as STOCK_CODE,
    BACKTEST_YEARS_EXPORT as BACKTEST_YEARS,
    END_DATE_EXPORT as END_DATE,
    get_year_range
)


INITIAL_CAPITAL = 100000


def is_trend_start(df: pd.DataFrame, i: int) -> bool:
    if i < 2:
        return False

    ma20 = df.loc[i, 'ma20']
    ma20_prev1 = df.loc[i-1, 'ma20']
    ma20_prev2 = df.loc[i-2, 'ma20']

    return ma20 > ma20_prev1 > ma20_prev2


def is_first_pullback(df: pd.DataFrame, i: int, state: dict) -> bool:
    if i < 1:
        return False

    if state is None or 'b1_done' not in state:
        return False

    close = df.loc[i, 'close']
    ma20 = df.loc[i, 'ma20']

    if not state['b1_done']:
        return False

    if state.get('b2_done'):
        return False

    return close > ma20 and close < state['b1_price'] * 1.05


def is_new_high(df: pd.DataFrame, i: int, state: dict) -> bool:
    if i < 1:
        return False

    if state is None or 'b1_done' not in state:
        return False

    if state.get('b3_done'):
        return False

    close = df.loc[i, 'close']
    entry_high = state.get('entry_high', state['b1_price'])

    return close > entry_high


def run_backtest(stock_code: str = STOCK_CODE):
    start_year, end_year = get_year_range(BACKTEST_YEARS)
    df = get_weekly_data(stock_code, days=365 * BACKTEST_YEARS)
    df = calculate_indicators(df)

    if len(df) < 21:
        print(f"数据不足，需要至少21周，当前只有{len(df)}周")
        return None, {}, {}

    print(f"\n{'='*100}")
    print(f"股票代码: {stock_code}")
    print(f"回测区间: {start_year} - {end_year} ({BACKTEST_YEARS}年)")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"B1: 趋势确认 (50%)")
    print(f"B2: 首次回踩 (30%)")
    print(f"B3: 再创新高 (20%)")
    print(f"卖出: +30%卖30%, +60%卖30%, 跌破MA20清仓")
    print(f"{'='*100}\n")

    cash = INITIAL_CAPITAL
    shares = 0
    state = None
    sell_state = None

    yearly_summary = {}
    last_year = None
    prev_year_end_asset = INITIAL_CAPITAL

    print(f"{'周序号':<6} {'日期':<12} {'收盘价':>10} {'MA20':>10} {'状态':<10} {'持仓':>10} {'现金':>12} {'总资产':>12} {'操作'}")
    print("-" * 100)

    for i in range(len(df)):
        row = df.iloc[i]
        current_date = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else ''
        current_price = row['close']
        ma20 = row['ma20']

        total_asset = cash + shares * current_price
        if state is None:
            current_state = "空仓"
        else:
            state_parts = []
            if state.get('b1_done'): state_parts.append("B1")
            if state.get('b2_done'): state_parts.append("B2")
            if state.get('b3_done'): state_parts.append("B3")
            if state.get('sell30_done'): state_parts.append("S30")
            if state.get('sell60_done'): state_parts.append("S60")
            current_state = "+".join(state_parts) if state_parts else "趋势中"

        operation = ""

        if state is None:
            if is_trend_start(df, i):
                state = {
                    'b1_done': False,
                    'b2_done': False,
                    'b3_done': False,
                    'b1_price': 0,
                    'entry_high': current_price,
                    'sell30_done': False,
                    'sell60_done': False
                }

        if state is not None:
            entry_price = state.get('b1_price', 0)

            if not state['b1_done']:
                buy_ratio = 0.5
                buy_amount = cash * buy_ratio
                new_shares = int(buy_amount / current_price)

                if new_shares > 0:
                    shares = new_shares
                    cash -= new_shares * current_price
                    state['b1_done'] = True
                    state['b1_price'] = current_price
                    state['entry_high'] = current_price
                    operation = f"B1买入50% {new_shares}股@{current_price:.2f}"

            elif not state['b2_done']:
                if is_first_pullback(df, i, state):
                    buy_ratio = 0.3
                    buy_amount = cash * buy_ratio
                    new_shares = int(buy_amount / current_price)

                    if new_shares > 0:
                        shares += new_shares
                        cash -= new_shares * current_price
                        state['b2_done'] = True
                        operation = f"B2回踩买入30% {new_shares}股@{current_price:.2f}"

            elif not state['b3_done']:
                if is_new_high(df, i, state):
                    buy_ratio = 0.2
                    buy_amount = cash * buy_ratio
                    new_shares = int(buy_amount / current_price)

                    if new_shares > 0:
                        shares += new_shares
                        cash -= new_shares * current_price
                        state['b3_done'] = True
                        state['entry_high'] = current_price
                        operation = f"B3创新高买入20% {new_shares}股@{current_price:.2f}"

            if state is not None and state['b1_done'] and current_state != "空仓":
                entry_price = state['b1_price']
                entry_high = state['entry_high']

                if current_price > entry_price * 1.30 and not state.get('sell30_done'):
                    sell_shares = int(shares * 0.3)
                    if sell_shares > 0:
                        cash += sell_shares * current_price
                        shares -= sell_shares
                        state['sell30_done'] = True
                        state['entry_high'] = current_price
                        operation = f"+30%卖出30% {sell_shares}股@{current_price:.2f}"

                elif current_price > entry_price * 1.60 and not state.get('sell60_done'):
                    sell_shares = int(shares * 0.3)
                    if sell_shares > 0:
                        cash += sell_shares * current_price
                        shares -= sell_shares
                        state['sell60_done'] = True
                        operation = f"+60%卖出30% {sell_shares}股@{current_price:.2f}"

                if current_price < ma20 and shares > 0:
                    cash += shares * current_price
                    total_asset = cash
                    operation = f"跌破MA20清仓 {shares}股@{current_price:.2f}"
                    shares = 0
                    state = None

                elif current_price > entry_high and state is not None:
                    state['entry_high'] = current_price

        print(f"{i+1:<6} {current_date:<12} {current_price:>10.2f} {ma20:>10.2f} {current_state:<10} {shares:>10} {cash:>12.2f} {total_asset:>12.2f} {operation}")

        if operation and '清仓' in operation:
            continue

        current_year = row['date'].year if pd.notna(row['date']) else None
        
        if current_year is not None:
            if last_year is None:
                last_year = current_year
                yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
            
            if current_year != last_year:
                yearly_summary[last_year]['end_asset'] = total_asset
                prev_year_end_asset = total_asset
                yearly_summary[current_year] = {'start_asset': prev_year_end_asset, 'end_asset': None}
                last_year = current_year
            
            yearly_summary[current_year]['end_asset'] = total_asset

    final_asset = cash + shares * df.iloc[-1]['close']

    print(f"\n{'='*100}")
    print(f"起始资金: {INITIAL_CAPITAL:.2f} 元")
    print(f"最终资产: {final_asset:.2f} 元")
    print(f"收益率: {(final_asset / INITIAL_CAPITAL - 1) * 100:.2f}%")
    print(f"{'='*100}\n")
    
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
    return total_return, yearly_returns, {}


if __name__ == "__main__":
    total_return, yearly_returns, extra = run_backtest()
    print(f"\n汇总结果:")
    print(f"总收益率: {total_return:+.2f}%")
    print(f"年度收益率: {yearly_returns}")
