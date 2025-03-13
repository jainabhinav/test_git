from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_454(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Category"), 
        col("`Gl Account`").alias("Gl Account"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        regexp_replace(col("`Planning Account`"), "(?i)(PA[0-9]{6}) - .*", "$1")\
          .cast(StringType())\
          .alias(
          "#PA"
        )
    )
