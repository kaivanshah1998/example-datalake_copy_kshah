from beyond_bets.base.time_series import TimeSeriesTransform
from beyond_bets.datasets.bets import Bets


class PlayerDaily(TimeSeriesTransform):
    def __init__(self):
        super().__init__(
            group_by_cols=["player_id"],
            time_unit="day"
        )
        self._name = "PlayerDaily"
        self._inputs = {"bets": Bets()}
