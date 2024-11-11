# Beyond Bets

##  Updates
Please run the demo.py to see the reflected changes

Below are the improvements to the codebase:

### Core Framework Improvements

- **Base Time Series Framework**: Introduced `TimeSeriesTransform` base class to eliminate code duplication across time-based aggregations (PlayerDaily, MarketHourly, etc.), making the codebase more maintainable.

- **Standardized Time Aggregations**: Unified the approach to time-based aggregations by making time units ("hour" or "day") and grouping columns configurable parameters, reducing the chance of inconsistencies.

### New Analytics Features

- **VIP Analytics**: Implemented `TopPlayers` transform to identify the top 1% of players by spend in the last week, using window functions for percentile calculations.

- **Market Analysis Capability**: Added `BetGrader` transform with a 15-minute rolling window to analyze bet sizes relative to market averages.

### Future Improvements

#### Architecture & Data Quality
- **Medallion Architecture**: Implement proper bronze/silver/gold layer structure to separate ingestion, transformation, and aggregation stages.
- **Data Governance**: Add comprehensive QC checks and data quality monitoring throughout the pipeline.
- **Testing Framework**: Develop full suite of unit tests, functional tests, and end-to-end testing scenarios.

#### Infrastructure & Performance
- **Infrastructure Sizing**: Properly size compute resources based on workload patterns rather than relying solely on auto-scaling.
- **Data Management**: Implement sophisticated merge/upsert strategies for handling data appends, particularly for slowly changing dimensions.

#### Spark Optimizations
- **Caching Strategy**: Implement strategic dataframe caching when dataset sizes grow beyond current scale.
- **Performance Tuning**: Fine-tune Spark configurations based on actual production workload characteristics.


