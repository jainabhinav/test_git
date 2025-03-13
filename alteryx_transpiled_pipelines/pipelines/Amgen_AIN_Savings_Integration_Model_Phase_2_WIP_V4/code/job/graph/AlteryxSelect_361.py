from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_361(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("AIN_Savings"), 
        col("`Adjustment Required?`").alias("Adjustment Required?"), 
        col("Year"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Pulled Using`").alias("Pulled Using"), 
        col("Function"), 
        col("EVP"), 
        col("HLMC"), 
        col("`Cost Center`").alias("Cost Center")
    )
