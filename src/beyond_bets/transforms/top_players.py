from pyspark.sql import DataFrame
from beyond_bets.base.transform import Transform
from beyond_bets.datasets.bets import Bets
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class TopPlayers(Transform):
    def __init__(self):
        super().__init__()
        self._name = "TopPlayers"
        self._inputs = {"bets": Bets()}

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
       
        df = (
            self.bets
            .where(
                F.col("timestamp") >= F.date_sub(F.current_date(), 7)
            )
            .groupBy("player_id")
            .agg(F.sum("bet_amount").alias("total_spend"))
        )
        
        window = Window.orderBy(F.col("total_spend").desc())
        df = df.withColumn("rank", F.percent_rank().over(window))
        
        return df.where(F.col("rank") <= 0.01).drop("rank") 