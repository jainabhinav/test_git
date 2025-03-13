from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_228(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Function"), 
        col("Name"), 
        col("Value"), 
        col("_seq"), 
        explode(slice(expr("split(Name, '[-]', 4)"), 1, 3)).alias("_temp_Name")
    )
