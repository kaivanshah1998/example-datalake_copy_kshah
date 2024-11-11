from pyspark.sql import DataFrame
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class BetGrader(Transform):
    def __init__(self):
        super().__init__()
        self._name = "BetGrader"
        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        df = self.bets.withColumn(
            "unix_timestamp", 
            F.unix_timestamp("timestamp")
        )      
        window_spec = (
            Window
            .partitionBy("market")
            .orderBy("unix_timestamp")
            .rangeBetween(-900, 0)  # 15 minutes in seconds
        )
        
        return (
            df
            .withColumn("market_avg", F.avg("bet_amount").over(window_spec))
            .withColumn("grade", F.col("bet_amount") / F.col("market_avg"))
            .select(
                "timestamp",
                "market",
                "bet_amount",
                "player_id",
                "market_avg",
                "grade"
            )
            .na.drop()  # Remove any rows where we couldn't calculate the average
        )