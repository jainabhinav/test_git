from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_327(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`Unique Key`").alias("Unique Key"))

    return df1.agg(
        count(when(((col("`Unique Key`") == lit(None)) | (col("`Unique Key`") == lit(""))), lit(1)).otherwise(lit(None)))\
          .alias("Count")
    )
