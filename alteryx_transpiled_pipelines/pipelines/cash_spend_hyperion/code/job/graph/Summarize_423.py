from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_423(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(lit(1).alias("dummyGroup"))

    return df1.agg(sum(col("Sum_LCLCFX")).alias("Sum_Sum_LCLCFX"))
