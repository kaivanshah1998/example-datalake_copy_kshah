from beyond_bets.base.time_series import TimeSeriesTransform
from beyond_bets.datasets.bets import Bets


class MarketDaily(TimeSeriesTransform):
    def __init__(self):
        super().__init__(
            group_by_cols=["market"],
            time_unit="day"
        )
        self._name = "MarketDaily"
        self._inputs = {"bets": Bets()} 