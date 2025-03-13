from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_231(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`EW Reduction-2027-Cumulative`").alias("EW Reduction-2027-Cumulative"), 
        col("`AIN Resource Cost-2027-Incremental`").alias("AIN Resource Cost-2027-Incremental"), 
        col("`AIN Savings-2025-Incremental`").alias("AIN Savings-2025-Incremental"), 
        col("`EW Reduction-2029-Incremental`").alias("EW Reduction-2029-Incremental"), 
        col("`AIN Savings-2025-Cumulative`").alias("AIN Savings-2025-Cumulative"), 
        col("`EW Reduction-2024-Incremental`").alias("EW Reduction-2024-Incremental"), 
        col("`EW Reduction-2026-Cumulative`").alias("EW Reduction-2026-Cumulative"), 
        col("`AIN Resource Cost-2027-Cumulative`").alias("AIN Resource Cost-2027-Cumulative"), 
        col("`EW Reduction-2029-Cumulative`").alias("EW Reduction-2029-Cumulative"), 
        col("`EW Reduction-2027-Incremental`").alias("EW Reduction-2027-Incremental"), 
        col("`AIN Savings-2024-Cumulative`").alias("AIN Savings-2024-Cumulative"), 
        col("`AIN Savings-2024-Incremental`").alias("AIN Savings-2024-Incremental"), 
        col("`EW Reduction-2025-Cumulative`").alias("EW Reduction-2025-Cumulative"), 
        col("`AIN Resource Cost-2024-Incremental`").alias("AIN Resource Cost-2024-Incremental"), 
        col("`AIN Resource Cost-2025-Incremental`").alias("AIN Resource Cost-2025-Incremental"), 
        col("`AIN Resource Cost-2029-Incremental`").alias("AIN Resource Cost-2029-Incremental"), 
        col("`EW Reduction-2028-Incremental`").alias("EW Reduction-2028-Incremental"), 
        col("`AIN Savings-2026-Cumulative`").alias("AIN Savings-2026-Cumulative"), 
        col("`AIN Resource Cost-2029-Cumulative`").alias("AIN Resource Cost-2029-Cumulative"), 
        col("`AIN Resource Cost-2026-Incremental`").alias("AIN Resource Cost-2026-Incremental"), 
        col("`EW Reduction-2030-Incremental`").alias("EW Reduction-2030-Incremental"), 
        col("`EW Reduction-2025-Incremental`").alias("EW Reduction-2025-Incremental"), 
        col("`AIN Resource Cost-2030-Cumulative`").alias("AIN Resource Cost-2030-Cumulative"), 
        col("`EW Reduction-2024-Cumulative`").alias("EW Reduction-2024-Cumulative"), 
        col("`AIN Resource Cost-2025-Cumulative`").alias("AIN Resource Cost-2025-Cumulative"), 
        col("`AIN Resource Cost-2024-Cumulative`").alias("AIN Resource Cost-2024-Cumulative"), 
        col("`AIN Resource Cost-2028-Incremental`").alias("AIN Resource Cost-2028-Incremental"), 
        col("`AIN Savings-2028-Cumulative`").alias("AIN Savings-2028-Cumulative"), 
        col("`AIN Resource Cost-2026-Cumulative`").alias("AIN Resource Cost-2026-Cumulative"), 
        col("`AIN Savings-2028-Incremental`").alias("AIN Savings-2028-Incremental"), 
        col("`EW Reduction-2026-Incremental`").alias("EW Reduction-2026-Incremental"), 
        col("`AIN Resource Cost-2030-Incremental`").alias("AIN Resource Cost-2030-Incremental"), 
        col("`AIN Savings-2027-Cumulative`").alias("AIN Savings-2027-Cumulative"), 
        col("`AIN Savings-2030-Cumulative`").alias("AIN Savings-2030-Cumulative"), 
        col("`AIN Savings-2026-Incremental`").alias("AIN Savings-2026-Incremental"), 
        col("`EW Reduction-2028-Cumulative`").alias("EW Reduction-2028-Cumulative"), 
        col("`AIN Resource Cost-2028-Cumulative`").alias("AIN Resource Cost-2028-Cumulative"), 
        col("`AIN Savings-2029-Incremental`").alias("AIN Savings-2029-Incremental"), 
        col("Function"), 
        col("`EW Reduction-2030-Cumulative`").alias("EW Reduction-2030-Cumulative"), 
        col("`AIN Savings-2027-Incremental`").alias("AIN Savings-2027-Incremental"), 
        col("`AIN Savings-2029-Cumulative`").alias("AIN Savings-2029-Cumulative"), 
        col("`AIN Savings-2030-Incremental`").alias("AIN Savings-2030-Incremental")
    )
