from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_65(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Sum_Sum_Spend ($)`").alias("Sum_Sum_Spend ($)"), 
        col("`tableau display category`").alias("tableau display category")
    )
