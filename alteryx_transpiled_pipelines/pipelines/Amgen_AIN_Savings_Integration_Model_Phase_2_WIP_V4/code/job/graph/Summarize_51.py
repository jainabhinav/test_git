from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_51(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Key"))

    return df1.agg(sum(col("`Sum_Sum_Sum_Spend ($)`")).alias("Sum_Sum_Sum_Sum_Spend ($)"))
