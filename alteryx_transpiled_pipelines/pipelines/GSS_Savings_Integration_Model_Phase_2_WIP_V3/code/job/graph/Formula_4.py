from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Right_Mega Category`").alias("Right_Mega Category"), 
        col("`Incremental Savings High`").alias("Incremental Savings High"), 
        col("`Mega Category`").alias("Mega Category"), 
        col("Year"), 
        col("EVP"), 
        col("`BAU Savings`").alias("BAU Savings"), 
        col("Right_Year"), 
        col("`Incremental Savings Low`").alias("Incremental Savings Low"), 
        col("`% Allocation - Incremental`").alias("% Allocation - Incremental"), 
        col("`% Allocation - BAU`").alias("% Allocation - BAU"), 
        (col("`% Allocation - BAU`") * col("`BAU Savings`")).cast(DoubleType()).alias("BAU Savings Split"), 
        (col("`Incremental Savings Low`") * col("`% Allocation - Incremental`"))\
          .cast(DoubleType())\
          .alias("Incremental Savings Low Split"), 
        (col("`Incremental Savings High`") * col("`% Allocation - Incremental`"))\
          .cast(DoubleType())\
          .alias("Incremental Savings High Split")
    )
