from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_108(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`Hyp Cat`").alias("Hyp Cat"), 
        col("`pa account`").alias("pa account"), 
        col("`pa account desc`").alias("pa account desc")
    )

    return df1.agg(sum(col("Sum_LCL")).alias("Avg_Sum_LCL"))
