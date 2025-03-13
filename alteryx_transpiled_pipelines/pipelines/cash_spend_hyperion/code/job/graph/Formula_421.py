from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_421(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("scenario name", lit("CASH SPEND 2023").cast(StringType()))\
        .withColumn(
          "quarter",
          call_spark_fcn("string_substring", col("quarter"), (length(col("quarter")) - lit(1)), lit(1))\
            .cast(StringType())
        )\
        .withColumn(
          "company description",
          concat(col("`company code number`"), lit("-"), col("`Company Description`")).cast(StringType())
        )\
        .withColumn(
          "GL Account Description Updated",
          regexp_replace(col("`GL Account Description Updated`"), "(?i)^\\d+\\s-\\s(.*)", "$1").cast(StringType())
        )\
        .withColumn("pa account", regexp_replace(col("`pa account`"), "(?i)(PA[0-9]{6}) - .*", "$1").cast(StringType()))\
        .withColumn("pa account desc", col("`pa account`").cast(StringType()))
