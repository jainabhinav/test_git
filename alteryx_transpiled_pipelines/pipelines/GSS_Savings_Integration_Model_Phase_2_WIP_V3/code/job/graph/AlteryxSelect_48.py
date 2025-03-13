from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_48(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Mega Category`").alias("Mega Category"), 
        col("Year"), 
        col("`% Allocation - BAU`").alias("% Allocation - BAU"), 
        col("`% Allocation - Incremental`").alias("% Allocation - Incremental"), 
        col("EVP")
    )
