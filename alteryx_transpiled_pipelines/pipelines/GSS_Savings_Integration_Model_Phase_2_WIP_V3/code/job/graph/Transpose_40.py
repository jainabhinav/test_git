from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_40(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

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
        ["Mega Category", "Savings Type"],
        ["R&D/MA-2025", "Operations-2025", "S&M/Int'l-2025", "G&A-2025", "Other-2025", "R&D/MA-2026", "Operations-2026",
         "S&M/Int'l-2026", "G&A-2026", "Other-2026", "R&D/MA-2027", "Operations-2027", "S&M/Int'l-2027",
         "G&A-2027", "Other-2027"]
    )

    return out0
