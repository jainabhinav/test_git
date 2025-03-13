from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_299(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "HLMC",
        when((col("HLMC") == lit("Chief Medical Office")).cast(BooleanType()), lit("ATMOS"))\
          .otherwise(col("HLMC"))\
          .cast(StringType())
    )
