from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_59(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Planning Account Actual",
          when(
              lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), 
              lit("PA Not Available (LRP)")
            )\
            .otherwise(col("`Planning Account Actual`"))\
            .cast(StringType())
        )\
        .withColumn(
          "Planning Account Description Actual",
          when(
              lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), 
              lit("PA Description Not Available (LRP)")
            )\
            .otherwise(col("`Planning Account Description Actual`"))\
            .cast(StringType())
        )\
        .withColumn(
          "Company Group",
          when((col("`Company Code`") < lit(2000)).cast(BooleanType()), lit("Amgen"))\
            .otherwise(lit("Horizon"))\
            .cast(StringType())
        )\
        .withColumn("In-Scope Final", lit("Addressable").cast(StringType()))
