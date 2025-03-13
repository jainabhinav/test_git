from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_68_cast(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Gl Account`").cast(StringType()).alias("Gl Account"), 
        col("Category").cast(StringType()).alias("Category"), 
        col("`Regrouped Level 4`").cast(StringType()).alias("Regrouped Level 4"), 
        col(
            "`Gl Account (# only)`"
          )\
          .cast(IntegerType())\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`Planning Account`").cast(StringType()).alias("Planning Account")
    )
