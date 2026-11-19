"""Indian intraday equity (MIS) charges — approximate, discount-broker schedule."""


def intraday_charges(buy_value: float, sell_value: float) -> float:
    brokerage = min(20.0, 0.0003 * buy_value) + min(20.0, 0.0003 * sell_value)
    stt = 0.00025 * sell_value
    exch = 0.0000297 * (buy_value + sell_value)
    sebi = 10 / 1e7 * (buy_value + sell_value)
    gst = 0.18 * (brokerage + exch + sebi)
    stamp = 0.00003 * buy_value
    return round(brokerage + stt + exch + sebi + gst + stamp, 2)
