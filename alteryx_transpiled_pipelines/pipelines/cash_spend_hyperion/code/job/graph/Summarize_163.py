from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_163(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("category"), col("Flag"))

    return df1.agg(sum(col("LCLCFX")).alias("Sum_LCLCFX"))
