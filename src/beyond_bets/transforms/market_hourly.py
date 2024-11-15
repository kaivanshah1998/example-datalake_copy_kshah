from beyond_bets.base.time_series import TimeSeriesTransform
from beyond_bets.datasets.bets import Bets


class MarketHourly(TimeSeriesTransform):
    def __init__(self):
        super().__init__(
            group_by_cols=["market"],
            time_unit="hour"
        )
        self._name = "MarketHourly"
        self._inputs = {"bets": Bets()}
