from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_194_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`1`").alias("1"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`2`").alias("2")
    )
