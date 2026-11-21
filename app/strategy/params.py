"""All strategy parameters — editable from the Settings page, stored (versioned) in Mongo `strategy_config`."""
from typing import Literal
from pydantic import BaseModel, Field, model_validator


class CEParams(BaseModel):
    length: int = 22
    mult: float = 3.0
    use_close: bool = True


class VPParams(BaseModel):
    rows: int = 24
    value_area_pct: float = 70.0


MASource = Literal["open", "high", "low", "close", "volume", "hl2", "hlc3", "ohlc4", "hlcc4"]
MAKind = Literal["SMA", "EMA", "SMMA", "RMA", "WMA", "VWMA"]


class MASpec(BaseModel):
    """One line of the TradingView "MA ribbon" indicator: on/off, type, source, length."""
    enabled: bool = True
    kind: MAKind = "SMA"
    source: MASource = "close"
    length: int = Field(default=20, ge=1, le=1000)


def _default_mas() -> list[MASpec]:
    return [MASpec(length=n) for n in (20, 50, 100, 200)]


class RibbonParams(BaseModel):
    mas: list[MASpec] = Field(default_factory=_default_mas)

    @model_validator(mode="before")
    @classmethod
    def _migrate_old(cls, v):
        # settings saved before per-MA options existed: {lengths, kind, source}
        if isinstance(v, dict) and "mas" not in v and any(k in v for k in ("lengths", "kind", "source")):
            return {"mas": [{"enabled": True, "kind": v.get("kind", "SMA"), "source": v.get("source", "close"),
                             "length": L} for L in v.get("lengths", [20, 50, 100, 200])]}
        return v

    @property
    def lengths(self) -> list[int]:
        return [m.length for m in self.mas if m.enabled]


class ScanParams(BaseModel):
    month_lookback_days: int = 20          # "new month high" = prev day's high >= highest high of last N sessions
    month_basis: Literal["high_low", "close"] = "high_low"
    top_n: int = 3


class RegimeParams(BaseModel):
    threshold_points: float = 50.0
    # literal  : change > +threshold -> BUY, else SELL           (client's wording)
    # symmetric: >= +threshold BUY, <= -threshold SELL, else NO_TRADE
    rule: Literal["literal", "symmetric"] = "literal"
    decision_time: str = "09:30"


class Filter1Params(BaseModel):
    enabled: bool = True
    timeframe: Literal["1W", "1D", "1M"] = "1W"
    years: int = 5
    ce: CEParams = Field(default_factory=CEParams)
    vp: VPParams = Field(default_factory=VPParams)
    use_heikin_ashi: bool = True


class Filter2Params(BaseModel):
    enabled: bool = True
    ce: CEParams = Field(default_factory=CEParams)
    ribbon: RibbonParams = Field(default_factory=RibbonParams)
    # candle values that must ALL be beyond the ribbon (above upper band for BUY / below lower band for SELL)
    candle_fields: list[Literal["open", "high", "low", "close"]] = Field(default_factory=lambda: ["open", "close"])
    setup_requires_fresh_signal: bool = False  # True: CE buy *signal* must print on the setup candle; False: CE BUY state
    setup_lookback_days: int = 10              # how old the setup candle may be
    use_heikin_ashi: bool = True


class Filter3Params(BaseModel):
    enabled: bool = True
    ce: CEParams = Field(default_factory=CEParams)
    vp: VPParams = Field(default_factory=VPParams)
    above_poc_rule: Literal["close", "open_close"] = "close"
    use_heikin_ashi: bool = True


class RiskParams(BaseModel):
    capital_per_trade: float = 100000.0
    max_open_positions: int = 3
    one_trade_per_stock_per_day: bool = True
    stop_loss_type: Literal["none", "ce_stop", "percent"] = "percent"
    stop_loss_pct: float = 1.0
    target_pct: float = 0.0                   # 0 = no target
    square_off_time: str = "15:15"
    last_entry_time: str = "14:45"
    daily_loss_limit: float = 5000.0          # stop new entries & square off when day P&L <= -limit (0 = off)
    slippage_pct: float = 0.02


class StrategyParams(BaseModel):
    scan: ScanParams = Field(default_factory=ScanParams)
    regime: RegimeParams = Field(default_factory=RegimeParams)
    filter1: Filter1Params = Field(default_factory=Filter1Params)
    filter2: Filter2Params = Field(default_factory=Filter2Params)
    filter3: Filter3Params = Field(default_factory=Filter3Params)
    risk: RiskParams = Field(default_factory=RiskParams)
    sides_enabled: list[Literal["BUY", "SELL"]] = Field(default_factory=lambda: ["BUY", "SELL"])
