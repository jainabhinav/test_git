from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_13(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Category"), 
        col("`Gl Account`").alias("Gl Account"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col(
            "`Gl Account (# only)`"
          )\
          .cast(StringType())\
          .alias(
          "Gl Account (# only)"
        )
    )
