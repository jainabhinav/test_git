from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`Mega Category`").alias("Mega Category"))

    return df1.agg(
        sum(col("`BAU Savings Split`")).alias("Sum_BAU Savings Split"), 
        sum(col("`Incremental Savings Low Split`")).alias("Sum_Incremental Savings Low Split"), 
        sum(col("`Incremental Savings High Split`")).alias("Sum_Incremental Savings High Split")
    )
