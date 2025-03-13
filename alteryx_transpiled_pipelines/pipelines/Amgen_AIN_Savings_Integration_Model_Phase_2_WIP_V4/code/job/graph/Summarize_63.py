from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_63(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`tableau display mega category`").alias("tableau display mega category"))

    return df1.agg(lit(1).alias("_dummy_"))
