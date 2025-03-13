from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_9(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Mega Category`").alias("Mega Category"), 
        col("`Incremental Savings High`").alias("Incremental Savings High"), 
        col("Year"), 
        col("`BAU Savings`").alias("BAU Savings"), 
        col("`% Allocation - BAU`").alias("% Allocation - BAU"), 
        col("`Incremental Savings Low`").alias("Incremental Savings Low"), 
        col("`% Allocation - Incremental`").alias("% Allocation - Incremental"), 
        col("EVP"), 
        col("`Incremental Savings Low Split`").alias("Incremental Savings Low Split"), 
        col("`Right_Mega Category`").alias("Right_Mega Category"), 
        col("`BAU Savings Split`").alias("BAU Savings Split"), 
        col("`Incremental Savings High Split`").alias("Incremental Savings High Split"), 
        col("Right_Year")
    )
