from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_69(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Right_tableau display mega category`").alias("Right_tableau display mega category"), 
        col("`Right_Sum_Sum_Sum_Spend ($)`").alias("Right_Sum_Sum_Sum_Spend ($)"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Sum_Sum_Sum_Spend ($)`").alias("Sum_Sum_Sum_Spend ($)"), 
        (col("`Sum_Sum_Sum_Spend ($)`") / col("`Right_Sum_Sum_Sum_Spend ($)`")).cast(DoubleType()).alias("%Split")
    )
