from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_20(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

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

    out0 = transpose_dataframe(in0, ["Function"], ["Complete", "On Track", "Delayed"])

    return out0
