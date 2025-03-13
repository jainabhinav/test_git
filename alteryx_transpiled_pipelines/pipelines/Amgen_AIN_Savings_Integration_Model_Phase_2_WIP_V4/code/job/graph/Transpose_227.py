from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_227(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def transpose_dataframe(df: DataFrame, key_columns: list, data_columns: list) -> DataFrame:
        available_data_columns = [col_name for col_name in data_columns if col_name in df.columns]
        dfs = [
                  df.select(
                    (
                      [col(key_col) for key_col in key_columns]
                      + [lit(data_col_name).cast("string").alias("Name"),
                                             col(data_col_name).cast("string").alias("Value")]
                    )
                  )
                  for data_col_name in available_data_columns
                  ]
        transposed_df = dfs[0]

        for other_df in dfs[1:]:
            transposed_df = transposed_df.union(other_df)

        return transposed_df

    out0 = transpose_dataframe(
        in0,
        ["Function"],
        ["EW Reduction-2024-Cumulative", "EW Reduction-2025-Cumulative", "EW Reduction-2026-Cumulative",
         "EW Reduction-2027-Cumulative", "EW Reduction-2028-Cumulative", "EW Reduction-2029-Cumulative",
         "EW Reduction-2030-Cumulative", "EW Reduction-2024-Incremental", "EW Reduction-2025-Incremental",
         "EW Reduction-2026-Incremental", "EW Reduction-2027-Incremental", "EW Reduction-2028-Incremental",
         "EW Reduction-2029-Incremental", "EW Reduction-2030-Incremental", "AIN Resource Cost-2024-Cumulative",
         "AIN Resource Cost-2025-Cumulative", "AIN Resource Cost-2026-Cumulative",
         "AIN Resource Cost-2027-Cumulative", "AIN Resource Cost-2028-Cumulative",
         "AIN Resource Cost-2029-Cumulative", "AIN Resource Cost-2030-Cumulative",
         "AIN Resource Cost-2024-Incremental", "AIN Resource Cost-2025-Incremental",
         "AIN Resource Cost-2026-Incremental", "AIN Resource Cost-2027-Incremental",
         "AIN Resource Cost-2028-Incremental", "AIN Resource Cost-2029-Incremental",
         "AIN Resource Cost-2030-Incremental", "AIN Savings-2024-Cumulative", "AIN Savings-2025-Cumulative",
         "AIN Savings-2026-Cumulative", "AIN Savings-2027-Cumulative", "AIN Savings-2028-Cumulative",
         "AIN Savings-2029-Cumulative", "AIN Savings-2030-Cumulative", "AIN Savings-2024-Incremental",
         "AIN Savings-2025-Incremental", "AIN Savings-2026-Incremental", "AIN Savings-2027-Incremental",
         "AIN Savings-2028-Incremental", "AIN Savings-2029-Incremental", "AIN Savings-2030-Incremental"]
    )

    return out0
