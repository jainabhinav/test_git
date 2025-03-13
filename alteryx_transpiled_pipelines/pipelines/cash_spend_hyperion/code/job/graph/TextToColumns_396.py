from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_396(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Planning Account`").alias("Planning Account"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("_seq"), 
        explode(expr("split(`Planning Account`, '[-]', 2)")).alias("_temp_Planning Account")
    )
