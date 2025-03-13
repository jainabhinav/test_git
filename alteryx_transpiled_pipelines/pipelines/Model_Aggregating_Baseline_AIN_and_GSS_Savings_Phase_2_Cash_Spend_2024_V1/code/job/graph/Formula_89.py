from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_89(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Cash Spend", col("Sum_Adjusted_Sum_LCLCFX").cast(DoubleType()))\
        .withColumn(
          "Category",
          when(
              lower(col("`scenario name`")).contains(lower(lit("cash spend"))).cast(BooleanType()), 
              col("`Hyperion Category`")
            )\
            .otherwise(lit(""))\
            .cast(StringType())
        )\
        .withColumn(
          "Hyperion Category",
          when(lower(col("`scenario name`")).contains(lower(lit("cash spend"))).cast(BooleanType()), lit(""))\
            .otherwise(col("`Hyperion Category`"))\
            .cast(StringType())
        )\
        .withColumn("Company Group", when((col("`Company Code`") < lit(2000)).cast(BooleanType()), lit("Amgen"))\
        .otherwise(lit("Horizon"))\
        .cast(StringType()))
