from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from beyond_bets.base.transform import Transform
from typing import Optional


class TimeSeriesTransform(Transform):
    """Base class for time-series aggregations"""
    
    def __init__(self, 
                 group_by_cols: list[str],
                 time_unit: str,
                 time_col: str = "timestamp",
                 agg_col: str = "bet_amount",
                 agg_func: str = "sum"):
        super().__init__()
        self.group_by_cols = group_by_cols
        self.time_unit = time_unit
        self.time_col = time_col
        self.agg_col = agg_col
        self.agg_func = agg_func

    def _transformation(self, **kwargs: dict[str, any]) -> DataFrame:
        
        if self.time_unit == "hour":
            time_col = F.date_trunc("hour", F.col(self.time_col))
        else:  # day
            time_col = F.to_date(F.col(self.time_col))
            
        df = df.withColumn("time_bucket", time_col)

        df_result = df.groupBy(*self.group_by_cols, "time_bucket").agg(
            F.expr(f"{self.agg_func}({self.agg_col})").alias("total_bets"))
        
        df_result = df_result.coalesce(2)
        return df_result
        