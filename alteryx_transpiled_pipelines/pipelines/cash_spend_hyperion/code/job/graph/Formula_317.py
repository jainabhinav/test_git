from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_317(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "Sum_Adjusted_Sum_LCLCFX",
          when(col("Sum_Adjusted_Sum_LCLCFX").isNull().cast(BooleanType()), lit(0))\
            .otherwise(col("Sum_Adjusted_Sum_LCLCFX"))\
            .cast(DoubleType())
        )\
        .withColumn(
          "Non-Opex Adjustments",
          when(col("`Non-Opex Adjustments`").isNull().cast(BooleanType()), lit(0))\
            .otherwise(col("`Non-Opex Adjustments`"))\
            .cast(DoubleType())
        )\
        .withColumn("Cash Spend", (col("Sum_Adjusted_Sum_LCLCFX") + col("`Non-Opex Adjustments`")).cast(DoubleType()))\
        .withColumn(
          "Planning Account Actual",
          when(lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), lit(""))\
            .otherwise(col("`Planning Account`"))\
            .cast(StringType())
        )\
        .withColumn("Planning Account Description Actual", when(lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), lit(""))\
        .otherwise(col("`Planning Account Description`"))\
        .cast(StringType()))
