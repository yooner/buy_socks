"""
市场六状态分析器
基于利弗莫尔市场关键点理论

六种状态:
- 上升趋势 (UP_TREND)
- 自然回升 (NATURAL_RALLY)
- 次级回升 (SECONDARY_RALLY)
- 下降趋势 (DOWN_TREND)
- 自然回撤 (NATURAL_REACTION)
- 次级回撤 (SECONDARY_REACTION)

转换规则:
- 6个点 = 20%
- 3个点 = 10%
"""

from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
import pandas as pd


class MarketState(Enum):
    """市场状态枚举"""
    UP_TREND = "上升趋势"
    NATURAL_RALLY = "自然回升"
    SECONDARY_RALLY = "次级回升"
    DOWN_TREND = "下降趋势"
    NATURAL_REACTION = "自然回撤"
    SECONDARY_REACTION = "次级回撤"
    UNKNOWN = "未知"


@dataclass
class StateSegment:
    """状态段落"""
    state: MarketState
    start_idx: int
    start_date: str
    start_price: float
    end_idx: Optional[int] = None
    end_date: Optional[str] = None
    end_price: Optional[float] = None
    key_point: Optional[float] = None  # 最高点或最低点


@dataclass
class DailyState:
    """每日状态记录"""
    idx: int
    date: str
    price: float
    state: MarketState
    is_segment_start: bool = False
    is_segment_end: bool = False
    key_point: Optional[float] = None  # 当前状态的关键点（最高点或最低点）
    ref_key_point: Optional[float] = None  # 参考关键点（自然回撤参考下降趋势最低点，自然回升参考上升趋势最高点）
    notes: str = ""
    allow_buy_down_to_rally: bool = True  # 下降趋势→自然回升时是否允许买入
    last_down_trend_low: Optional[float] = None  # 上一次下降趋势的最低点（用于自然回升突破前低买入）


class MarketStateAnalyzer:
    """市场状态分析器
    
    支持两种模式：
    1. 百分比模式：传入 six_points_pct 和 three_points_pct
    2. 固定数值模式：传入 six_points_func 和 three_points_func，根据价格动态获取固定数值
    """
    
    def __init__(self, six_points_pct: float = None, three_points_pct: float = None, 
                 six_points_func=None, three_points_func=None):
        # 转换阈值（可配置）
        # 百分比模式（旧模式）
        self.SIX_POINTS_PCT = six_points_pct      # 6个点对应的百分比
        self.THREE_POINTS_PCT = three_points_pct  # 3个点对应的百分比
        
        # 固定数值模式（新模式）：传入函数，根据当前价格获取固定数值（元）
        self.six_points_func = six_points_func    # 函数：根据价格获取6个点对应的固定数值
        self.three_points_func = three_points_func  # 函数：根据价格获取3个点对应的固定数值
        
        # 判断使用哪种模式
        self.use_fixed_value_mode = six_points_func is not None and three_points_func is not None
        
        self.segments: List[StateSegment] = []
        self.daily_states: List[DailyState] = []
        
        # 初始化状态跟踪变量
        self._initialize_state_tracking()
        
    def _get_six_points(self, price: float) -> float:
        """获取6个点对应的阈值
        
        根据模式不同，返回百分比
        
        固定数值模式下，将固定数值（元）除以价格转换为百分比
        
        Args:
            price: 当前价格
            
        Returns:
            6个点对应的阈值（百分比）
        """
        if self.use_fixed_value_mode:
            fixed_value = self.six_points_func(price)
            if fixed_value is None:
                return 0.20  # 默认 fallback 20%
            return fixed_value / price
        return self.SIX_POINTS_PCT
    
    def _get_three_points(self, price: float) -> float:
        """获取3个点对应的阈值
        
        根据模式不同，返回百分比
        
        固定数值模式下，将固定数值（元）除以价格转换为百分比
        
        Args:
            price: 当前价格
            
        Returns:
            3个点对应的阈值（百分比）
        """
        if self.use_fixed_value_mode:
            fixed_value = self.three_points_func(price)
            if fixed_value is None:
                return 0.10  # 默认 fallback 10%
            return fixed_value / price
        return self.THREE_POINTS_PCT

    def _initialize_state_tracking(self):
        """初始化状态跟踪变量"""
        # 各状态的关键点记录 - 记录历史上各状态的关键价位
        self.natural_rally_high: Optional[float] = None  # 自然回升历史最高点
        self.natural_reaction_low: Optional[float] = None  # 自然回撤历史最低点
        self.up_trend_high: Optional[float] = None  # 上升趋势历史最高点
        self.down_trend_low: Optional[float] = None  # 下降趋势历史最低点
        self.secondary_rally_high: Optional[float] = None  # 次级回升最高点
        self.secondary_reaction_low: Optional[float] = None  # 次级回撤最低点

        # 当前段落的关键点（用于状态内更新）
        self.current_natural_rally_high: Optional[float] = None  # 当前自然回升高点
        self.current_natural_reaction_low: Optional[float] = None  # 当前自然回撤低点

        # 上一次自然回升和自然回撤的关键点（用于判断次级状态）
        self.last_natural_rally_high: Optional[float] = None  # 上一次自然回升的最高点
        self.last_natural_reaction_low: Optional[float] = None  # 上一次自然回撤的最低点
        
        # 上一次上升趋势和下降趋势的关键点（用于显示参考）
        self.last_up_trend_high: Optional[float] = None  # 上一次上升趋势的最高点
        self.last_down_trend_low: Optional[float] = None  # 上一次下降趋势的最低点
        
        # 当前状态
        self.current_state: MarketState = MarketState.UNKNOWN
        self.current_segment: Optional[StateSegment] = None
        
        # 记录上一次状态转换的参考价格（用于显示转换信息）
        self.last_transition_ref_price: Optional[float] = None
        
    def analyze(self, df: pd.DataFrame, price_col: str = '收盘', date_col: str = 'date') -> pd.DataFrame:
        """
        分析数据框，为每一天标注状态
        
        Args:
            df: 包含价格数据的DataFrame
            price_col: 价格列名
            date_col: 日期列名
            
        Returns:
            添加了状态列的DataFrame
        """
        df = df.copy()
        prices = df[price_col].values
        dates = df[date_col].values
        
        # 初始化第一个状态
        self._initialize_first_state(prices, dates)
        
        # 遍历每一天进行状态判断
        for i in range(1, len(prices)):
            self._process_day(i, dates[i], prices[i])
        
        # 结束最后一个段落
        if self.current_segment:
            self.current_segment.end_idx = len(prices) - 1
            self.current_segment.end_date = str(dates[-1])[:10]
            self.current_segment.end_price = prices[-1]
        
        # 将状态添加到DataFrame
        df['market_state'] = [ds.state.value for ds in self.daily_states]
        df['is_segment_start'] = [ds.is_segment_start for ds in self.daily_states]
        df['is_segment_end'] = [ds.is_segment_end for ds in self.daily_states]
        df['key_point'] = [ds.key_point for ds in self.daily_states]
        df['ref_key_point'] = [ds.ref_key_point for ds in self.daily_states]
        df['allow_buy_down_to_rally'] = [ds.allow_buy_down_to_rally for ds in self.daily_states]
        df['state_notes'] = [ds.notes for ds in self.daily_states]
        df['last_down_trend_low'] = [ds.last_down_trend_low for ds in self.daily_states]
        
        return df
    
    def _initialize_first_state(self, prices: List[float], dates: List):
        """初始化第一个状态"""
        # 初始状态设为上升趋势
        self.current_state = MarketState.UP_TREND
        self.up_trend_high = prices[0]
        
        segment = StateSegment(
            state=MarketState.UP_TREND,
            start_idx=0,
            start_date=str(dates[0])[:10],
            start_price=prices[0],
            key_point=prices[0]
        )
        self.segments.append(segment)
        self.current_segment = segment
        
        daily = DailyState(
            idx=0,
            date=str(dates[0])[:10],
            price=prices[0],
            state=MarketState.UP_TREND,
            is_segment_start=True,
            key_point=prices[0],
            notes="初始状态"
        )
        self.daily_states.append(daily)
        
    def _reset_state_tracking(self):
        """重置状态跟踪变量（当进入新趋势时调用）"""
        self.current_natural_rally_high = None
        self.current_natural_reaction_low = None
        self.secondary_rally_high = None
    def _process_day(self, idx: int, date, price: float):
        """处理每一天的状态转换"""
        date_str = str(date)[:10]
        prev_price = self.daily_states[-1].price
        
        # 根据当前状态判断转换
        new_state = self._determine_state(price, prev_price, date_str)
        
        # 默认允许买入（仅在 下降趋势→自然回升 场景下按规则可能改为不允许）
        allow_buy_down_to_rally = True
        
        # 检查是否需要转换状态
        if new_state != self.current_state:
            # 获取前一段落的关键价格（用于转换信息）
            prev_state = self.current_state
            prev_key_point = self.current_segment.key_point if self.current_segment else prev_price
            prev_last_down_trend_low = self.last_down_trend_low

            # 记录“上一轮自然回升/自然回撤”的真实关键点（只在离开该段时更新）
            if prev_state == MarketState.NATURAL_RALLY and prev_key_point is not None:
                self.last_natural_rally_high = prev_key_point
                if self.natural_rally_high is None or prev_key_point > self.natural_rally_high:
                    self.natural_rally_high = prev_key_point
            elif prev_state == MarketState.NATURAL_REACTION and prev_key_point is not None:
                self.last_natural_reaction_low = prev_key_point
                if self.natural_reaction_low is None or prev_key_point < self.natural_reaction_low:
                    self.natural_reaction_low = prev_key_point
            
            # 结束当前段落（记录在当前天）
            self.current_segment.end_idx = idx
            self.current_segment.end_date = date_str
            self.current_segment.end_price = price
            
            # 记录上一次上升趋势/下降趋势的关键点
            if prev_state == MarketState.UP_TREND and self.up_trend_high:
                # 从上升趋势转换出去，记录上一次上升趋势的高点
                self.last_up_trend_high = self.up_trend_high
            elif prev_state == MarketState.DOWN_TREND and self.down_trend_low:
                # 从下降趋势转换出去，记录上一次下降趋势的低点
                self.last_down_trend_low = self.down_trend_low
            
            # 根据新状态初始化相应的跟踪变量
            if new_state == MarketState.NATURAL_REACTION:
                # 进入自然回撤，初始化当前自然回撤最低点
                self.current_natural_reaction_low = price
            elif new_state == MarketState.NATURAL_RALLY:
                # 进入自然回升，初始化当前自然回升高点
                self.current_natural_rally_high = price
            elif new_state == MarketState.SECONDARY_REACTION:
                # 进入次级回撤，初始化次级回撤最低点
                self.secondary_reaction_low = price
            elif new_state == MarketState.SECONDARY_RALLY:
                # 进入次级回升，初始化次级回升高点
                self.secondary_rally_high = price
            
            # 创建新段落
            key_point = self._get_key_point_for_state(new_state, price)
            new_segment = StateSegment(
                state=new_state,
                start_idx=idx,
                start_date=date_str,
                start_price=price,
                key_point=key_point
            )
            self.segments.append(new_segment)
            self.current_segment = new_segment
            
            # 记录状态转换
            self.current_state = new_state
            is_start = True
            is_end = True  # 同一天既是前一段落的结束，也是新段落的开始
            
            # 计算转换信息（记录在当前天 - 状态转换的当天）
            price_change = price - prev_key_point
            price_change_pct = (price_change / prev_key_point) * 100 if prev_key_point != 0 else 0
            notes = f"从{prev_state.value}→{new_state.value} | 前段关键点:{prev_key_point:.2f}→{price:.2f} ({price_change:+.2f}, {price_change_pct:+.2f}%)"
            
            # 下降趋势→自然回升：本轮下降趋势未跌破上一轮下降趋势低点时，才允许买入
            if prev_state == MarketState.DOWN_TREND and new_state == MarketState.NATURAL_RALLY:
                if prev_last_down_trend_low is not None and self.down_trend_low is not None and self.down_trend_low < prev_last_down_trend_low:
                    allow_buy_down_to_rally = False
                    notes += f" | 下行破前低({self.down_trend_low:.2f}<{prev_last_down_trend_low:.2f})，本次不买入"
        else:
            is_start = False
            is_end = False
            notes = ""
            # 更新关键点
            self._update_key_point(price)
        
        # 获取参考关键点
        ref_key_point = self._get_ref_key_point()
        
        daily = DailyState(
            idx=idx,
            date=date_str,
            price=price,
            state=self.current_state,
            is_segment_start=is_start,
            is_segment_end=is_end,
            key_point=self.current_segment.key_point if self.current_segment else None,
            ref_key_point=ref_key_point,
            notes=notes,
            allow_buy_down_to_rally=allow_buy_down_to_rally,
            last_down_trend_low=self.last_down_trend_low
        )
        self.daily_states.append(daily)
    
    def _determine_state(self, price: float, prev_price: float, date_str: str = "") -> MarketState:
        """根据当前价格判断状态"""
        # 获取当前价格对应的阈值
        six_points = self._get_six_points(price)
        three_points = self._get_three_points(price)
        
        if self.current_state == MarketState.UP_TREND:
            # 上升趋势 → 自然回撤: 最高点下降6个点(20%)
            # 注意：上升趋势只能转为自然回撤，不能直接转为次级回撤
            if self.up_trend_high:
                drop_pct = (self.up_trend_high - price) / self.up_trend_high
                if drop_pct >= six_points:
                    # 转为自然回撤
                    self.current_natural_reaction_low = price
                    # 更新历史最低点和上一次自然回撤低点
                    if self.last_natural_reaction_low is None or price < self.last_natural_reaction_low:
                        self.natural_reaction_low = price
                    return MarketState.NATURAL_REACTION
            # 更新上升趋势最高点
            if price > self.up_trend_high:
                self.up_trend_high = price
                self.current_segment.key_point = price
            return MarketState.UP_TREND
        
        elif self.current_state == MarketState.NATURAL_REACTION:
            # 自然回撤 → 下降趋势: 
            # 条件1: 相比上一次自然回撤低点下跌 THREE_POINTS_PCT
            # 条件2: 或者跌破上一轮下降趋势的最低点
            can_convert_to_downtrend = False
            ref_price_for_note = 0
            
            # 条件1: 相比上一次自然回撤低点下跌 THREE_POINTS_PCT
            if self.last_natural_reaction_low:
                drop_pct_from_last_natural_reaction_low = (self.last_natural_reaction_low - price) / self.last_natural_reaction_low
                if drop_pct_from_last_natural_reaction_low >= three_points:
                    can_convert_to_downtrend = True
                    ref_price_for_note = self.last_natural_reaction_low
            
            # 条件2: 跌破上一轮下降趋势的最低点
            if self.last_down_trend_low and price < self.last_down_trend_low:
                can_convert_to_downtrend = True
                ref_price_for_note = self.last_down_trend_low
            
            if can_convert_to_downtrend:
                self.last_transition_ref_price = ref_price_for_note  # 记录参考价格用于显示
                self.down_trend_low = price
                # 更新最低点记录
                if self.current_natural_reaction_low is None or price < self.current_natural_reaction_low:
                    self.current_natural_reaction_low = price
                    if self.natural_reaction_low is None or price < self.natural_reaction_low:
                        self.natural_reaction_low = price
                return MarketState.DOWN_TREND
            
            # 自然回撤 → 自然回升/次级回升/上升趋势: 从最低点上升6个点(20%)
            if self.current_natural_reaction_low:
                rise_pct = (price - self.current_natural_reaction_low) / self.current_natural_reaction_low
                if rise_pct >= six_points:
                    # 判断是否可以直接转为上升趋势（突破上一轮自然回升高点超过3个点）
                    if self.last_natural_rally_high and price > self.last_natural_rally_high:
                        rise_pct_from_last_rally = (price - self.last_natural_rally_high) / self.last_natural_rally_high
                        if rise_pct_from_last_rally >= three_points:
                            # 直接转为上升趋势
                            self.up_trend_high = price
                            if self.natural_rally_high is None or price > self.natural_rally_high:
                                self.natural_rally_high = price
                            self._reset_state_tracking()
                            return MarketState.UP_TREND
                    
                    # 判断是次级回升还是自然回升
                    # 使用上一次自然回升的高点来判断（不是历史最高点）
                    if self.last_natural_rally_high is None or price > self.last_natural_rally_high:
                        # 突破上一次自然回升高点，转为自然回升
                        self.current_natural_rally_high = price
                        # 更新历史最高点和上一次自然回升高点
                        if self.natural_rally_high is None or price > self.natural_rally_high:
                            self.natural_rally_high = price
                        return MarketState.NATURAL_RALLY
                    else:
                        # 没有突破上一次自然回升的高点，转为次级回升
                        self.secondary_rally_high = price
                        return MarketState.SECONDARY_RALLY
            
            return MarketState.NATURAL_REACTION
        
        elif self.current_state == MarketState.DOWN_TREND:
            # 下降趋势 → 自然回升: 最低点上升6个点(20%)
            # 注意：下降趋势只能转为自然回升，不能直接转为次级回升
            if self.down_trend_low:
                rise_pct = (price - self.down_trend_low) / self.down_trend_low
                if rise_pct >= six_points:
                    # 转为自然回升
                    self.current_natural_rally_high = price
                    # 更新历史自然回升高点和上一次自然回升高点
                    if self.natural_rally_high is None or price > self.natural_rally_high:
                        self.natural_rally_high = price
                    return MarketState.NATURAL_RALLY
            
            # 更新下降趋势最低点
            if self.down_trend_low is None or price < self.down_trend_low:
                self.down_trend_low = price
                self.current_segment.key_point = price
            return MarketState.DOWN_TREND
        
        elif self.current_state == MarketState.NATURAL_RALLY:
            # 自然回升 → 上升趋势:
            # 条件1: 突破上一轮自然回升最高点 THREE_POINTS_PCT
            # 条件2: 或者突破上升趋势最高点
            can_convert_to_uptrend = False
            ref_price_for_note = 0
            
            # 条件1: 突破上一轮自然回升最高点 THREE_POINTS_PCT
            if self.last_natural_rally_high:
                rise_pct_from_last_natural_rally_high = (price - self.last_natural_rally_high) / self.last_natural_rally_high
                if rise_pct_from_last_natural_rally_high >= three_points:
                    can_convert_to_uptrend = True
                    ref_price_for_note = self.last_natural_rally_high
            
            # 条件2: 突破上升趋势最高点
            if self.up_trend_high and price > self.up_trend_high:
                can_convert_to_uptrend = True
                ref_price_for_note = self.up_trend_high
            if can_convert_to_uptrend:
                self.last_transition_ref_price = ref_price_for_note  # 记录参考价格用于显示
                self.up_trend_high = price
                # 更新最高点记录
                if self.natural_rally_high is None or price > self.natural_rally_high:
                    self.natural_rally_high = price
                self._reset_state_tracking()  # 进入新趋势，重置跟踪变量
                return MarketState.UP_TREND
            
            # 自然回升 → 自然回撤/次级回撤/下降趋势: 从最高点下降6个点(20%)
            if self.current_natural_rally_high:
                drop_pct = (self.current_natural_rally_high - price) / self.current_natural_rally_high
                if drop_pct >= six_points:
                    # 判断是否可以直接转为下降趋势（跌破上一轮自然回撤低点超过3个点）
                    if self.last_natural_reaction_low and price < self.last_natural_reaction_low:
                        drop_pct_from_last_reaction = (self.last_natural_reaction_low - price) / self.last_natural_reaction_low
                        if drop_pct_from_last_reaction >= three_points:
                            # 直接转为下降趋势
                            self.down_trend_low = price
                            if self.natural_reaction_low is None or price < self.natural_reaction_low:
                                self.natural_reaction_low = price
                            self._reset_state_tracking()
                            return MarketState.DOWN_TREND
                    
                    # 判断是次级回撤还是自然回撤
                    # 使用自然回撤低点作为基准：跌破则自然回撤，否则次级回撤
                    if self.last_natural_reaction_low is None or price < self.last_natural_reaction_low:
                        # 跌破自然回撤低点，转为自然回撤
                        self.current_natural_reaction_low = price
                        # 更新历史最低点和上一次自然回撤低点
                        if self.natural_reaction_low is None or price < self.natural_reaction_low:
                            self.natural_reaction_low = price
                        return MarketState.NATURAL_REACTION
                    else:
                        # 没有跌破自然回撤低点，转为次级回撤
                        self.secondary_reaction_low = price
                        return MarketState.SECONDARY_REACTION
            
            return MarketState.NATURAL_RALLY
        
        elif self.current_state == MarketState.SECONDARY_RALLY:
            # 先更新次级回升最高点
            if self.secondary_rally_high is None or price > self.secondary_rally_high:
                self.secondary_rally_high = price
                self.current_segment.key_point = price
            
            # 次级回升 → 自然回升: 突破自然回升高点（历史最高点）
            # 使用历史记录的自然回升高点来判断
            if self.last_natural_rally_high and price >= self.last_natural_rally_high:
                self.current_natural_rally_high = price
                self.natural_rally_high = price
                return MarketState.NATURAL_RALLY
            
            # 次级回升 → 次级回撤: 从最高点下降6个点(20%)，但没有跌破自然回撤低点
            if self.secondary_rally_high:
                drop_pct = (self.secondary_rally_high - price) / self.secondary_rally_high
                if drop_pct >= six_points:
                    if self.last_natural_reaction_low and price >= self.last_natural_reaction_low:
                        # 没有跌破自然回撤低点，是次级回撤
                        self.secondary_reaction_low = price
                        return MarketState.SECONDARY_REACTION
                    elif self.last_natural_reaction_low is None or price < self.last_natural_reaction_low:
                        # 跌破自然回撤低点，转为自然回撤
                        self.current_natural_reaction_low = price
                        if self.natural_reaction_low is None or price < self.natural_reaction_low:
                            self.natural_reaction_low = price
                        return MarketState.NATURAL_REACTION
            
            return MarketState.SECONDARY_RALLY
        
        elif self.current_state == MarketState.SECONDARY_REACTION:
            # 次级回撤 → 自然回撤: 跌破自然回撤最低点
            if self.last_natural_reaction_low and price < self.last_natural_reaction_low:
                self.current_natural_reaction_low = price
                if self.natural_reaction_low is None or price < self.natural_reaction_low:
                    self.natural_reaction_low = price
                return MarketState.NATURAL_REACTION

            # 次级回撤 → 次级回升: 相比次级回撤自身低点上升 SIX_POINTS
            if self.secondary_reaction_low:
                rise_pct = (price - self.secondary_reaction_low) / self.secondary_reaction_low
                if rise_pct >= six_points:
                    # 使用历史上自然回升的最高点来判断是否突破
                    historical_rally_high = self.last_natural_rally_high
                    if historical_rally_high and price > historical_rally_high:
                        # 突破自然回升历史最高点，转为自然回升
                        self.current_natural_rally_high = price
                        if self.natural_rally_high is None or price > self.natural_rally_high:
                            self.natural_rally_high = price
                        return MarketState.NATURAL_RALLY
                    else:
                        # 没有突破自然回升历史最高点（或没有历史记录），是次级回升
                        self.secondary_rally_high = price
                        return MarketState.SECONDARY_RALLY

            # 更新次级回撤最低点
            if self.secondary_reaction_low is None or price < self.secondary_reaction_low:
                self.secondary_reaction_low = price
                self.current_segment.key_point = price
            return MarketState.SECONDARY_REACTION
        
        return self.current_state
    
    def _get_key_point_for_state(self, state: MarketState, price: float) -> float:
        """获取新状态的初始关键点"""
        if state in [MarketState.UP_TREND, MarketState.NATURAL_RALLY, MarketState.SECONDARY_RALLY]:
            return price  # 最高点
        else:
            return price  # 最低点

    def _get_transition_ref_price(self, prev_state: MarketState, new_state: MarketState) -> float:
        """获取状态转换的参考价格（用于计算转换时的价格变化）"""
        # 上升趋势 → 自然回撤: 参考上升趋势最高点
        if prev_state == MarketState.UP_TREND and new_state == MarketState.NATURAL_REACTION:
            return self.last_up_trend_high if self.last_up_trend_high else self.current_segment.key_point

        # 自然回撤 → 下降趋势: 使用记录的参考价格（可能是段落起点或下降趋势最低点）
        elif prev_state == MarketState.NATURAL_REACTION and new_state == MarketState.DOWN_TREND:
            if self.last_transition_ref_price:
                ref = self.last_transition_ref_price
                self.last_transition_ref_price = None  # 使用后清空
                return ref
            return self.current_segment.start_price

        # 自然回撤 → 自然回升/次级回升: 参考自然回撤最低点
        elif prev_state == MarketState.NATURAL_REACTION and new_state in [MarketState.NATURAL_RALLY, MarketState.SECONDARY_RALLY]:
            return self.current_natural_reaction_low if self.current_natural_reaction_low else self.current_segment.start_price

        # 自然回升 → 上升趋势: 使用记录的参考价格（可能是自然回升高点或上升趋势高点）
        elif prev_state == MarketState.NATURAL_RALLY and new_state == MarketState.UP_TREND:
            if self.last_transition_ref_price:
                ref = self.last_transition_ref_price
                self.last_transition_ref_price = None  # 使用后清空
                return ref
            return self.current_natural_rally_high if self.current_natural_rally_high else self.current_segment.start_price

        # 自然回升 → 自然回撤/次级回撤: 参考自然回升高点
        elif prev_state == MarketState.NATURAL_RALLY and new_state in [MarketState.NATURAL_REACTION, MarketState.SECONDARY_REACTION]:
            return self.current_natural_rally_high if self.current_natural_rally_high else self.current_segment.start_price

        # 下降趋势 → 自然回升: 参考下降趋势最低点
        elif prev_state == MarketState.DOWN_TREND and new_state == MarketState.NATURAL_RALLY:
            return self.last_down_trend_low if self.last_down_trend_low else self.current_segment.start_price

        # 次级回升 → 自然回升: 参考次级回升突破的历史自然回升高点
        elif prev_state == MarketState.SECONDARY_RALLY and new_state == MarketState.NATURAL_RALLY:
            return self.last_natural_rally_high if self.last_natural_rally_high else self.current_segment.start_price

        # 次级回撤 → 自然回撤: 参考次级回撤跌破的历史自然回撤低点
        elif prev_state == MarketState.SECONDARY_REACTION and new_state == MarketState.NATURAL_REACTION:
            return self.last_natural_reaction_low if self.last_natural_reaction_low else self.current_segment.start_price

        # 默认使用当前段落的关键点
        else:
            return self.current_segment.key_point if self.current_segment else 0
    
    def _update_key_point(self, price: float):
        """更新当前段落的关键点"""
        if not self.current_segment:
            return
            
        if self.current_state in [MarketState.UP_TREND, MarketState.NATURAL_RALLY, MarketState.SECONDARY_RALLY]:
            # 更新最高点
            if price > self.current_segment.key_point:
                self.current_segment.key_point = price
            # 同时更新状态特定的跟踪变量
            if self.current_state == MarketState.NATURAL_RALLY:
                if self.current_natural_rally_high is None or price > self.current_natural_rally_high:
                    self.current_natural_rally_high = price
            elif self.current_state == MarketState.UP_TREND:
                if self.up_trend_high is None or price > self.up_trend_high:
                    self.up_trend_high = price
            elif self.current_state == MarketState.SECONDARY_RALLY:
                if self.secondary_rally_high is None or price > self.secondary_rally_high:
                    self.secondary_rally_high = price
        else:
            # 更新最低点
            if price < self.current_segment.key_point:
                self.current_segment.key_point = price
            # 同时更新状态特定的跟踪变量
            if self.current_state == MarketState.NATURAL_REACTION:
                if self.current_natural_reaction_low is None or price < self.current_natural_reaction_low:
                    self.current_natural_reaction_low = price
            elif self.current_state == MarketState.DOWN_TREND:
                if self.down_trend_low is None or price < self.down_trend_low:
                    self.down_trend_low = price
            elif self.current_state == MarketState.SECONDARY_REACTION:
                if self.secondary_reaction_low is None or price < self.secondary_reaction_low:
                    self.secondary_reaction_low = price
    
    def _get_ref_key_point(self) -> Optional[float]:
        """获取参考关键点
        
        - 自然回撤区间：显示下降趋势的最低点
        - 自然回升区间：显示上升趋势的最高点
        - 次级回升区间：显示自然回升的最高点
        - 次级回撤区间：显示自然回撤的最低点
        - 上升趋势区间：显示上一次上升趋势的最高点
        - 下降趋势区间：显示上一次下降趋势的最低点
        - 其他状态：返回None
        """
        if self.current_state == MarketState.NATURAL_REACTION:
            # 自然回撤参考下降趋势最低点
            return self.last_down_trend_low
        elif self.current_state == MarketState.NATURAL_RALLY:
            # 自然回升参考上升趋势最高点
            return self.last_up_trend_high
        elif self.current_state == MarketState.SECONDARY_RALLY:
            # 次级回升参考自然回升高点
            return self.last_natural_rally_high
        elif self.current_state == MarketState.SECONDARY_REACTION:
            # 次级回撤参考自然回撤低点
            return self.last_natural_reaction_low
        elif self.current_state == MarketState.UP_TREND:
            # 上升趋势参考上一次上升趋势的最高点
            return self.last_up_trend_high
        elif self.current_state == MarketState.DOWN_TREND:
            # 下降趋势参考上一次下降趋势的最低点
            return self.last_down_trend_low
        return None
    
    def get_segments_summary(self) -> pd.DataFrame:
        """获取段落摘要"""
        data = []
        for seg in self.segments:
            data.append({
                '状态': seg.state.value,
                '开始日期': seg.start_date,
                '开始价格': seg.start_price,
                '结束日期': seg.end_date,
                '结束价格': seg.end_price,
                '关键点': seg.key_point,
                '持续天数': (seg.end_idx - seg.start_idx + 1) if seg.end_idx else '进行中'
            })
        return pd.DataFrame(data)
    
    def print_summary(self):
        """打印分析摘要"""
        print("\n" + "="*80)
        print("市场状态分析摘要")
        print("="*80)
        
        for seg in self.segments:
            key_point_type = "最高点" if seg.state in [
                MarketState.UP_TREND, MarketState.NATURAL_RALLY, MarketState.SECONDARY_RALLY
            ] else "最低点"
            
            duration = (seg.end_idx - seg.start_idx + 1) if seg.end_idx else '进行中'
            end_date = seg.end_date if seg.end_date else '现在'
            
            print(f"\n【{seg.state.value}】")
            print(f"  区间: {seg.start_date} ~ {end_date}")
            print(f"  价格: {seg.start_price:.2f} ~ {seg.end_price if seg.end_price else '...'}")
            print(f"  {key_point_type}: {seg.key_point:.2f}")
            print(f"  持续: {duration} 天")
        
        print("\n" + "="*80)


# 便捷函数
def analyze_market_states(df: pd.DataFrame, price_col: str = '收盘', date_col: str = 'date') -> pd.DataFrame:
    """
    分析市场状态的便捷函数
    
    Args:
        df: 价格数据DataFrame
        price_col: 价格列名
        date_col: 日期列名
        
    Returns:
        添加了状态列的DataFrame
    """
    analyzer = MarketStateAnalyzer()
    return analyzer.analyze(df, price_col, date_col)



