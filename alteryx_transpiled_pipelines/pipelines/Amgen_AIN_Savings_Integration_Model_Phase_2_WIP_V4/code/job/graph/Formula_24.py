from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_24(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "CTS v3",
          when(((length(col("`CTS v3`")) == lit(0)) | col("`CTS v3`").isNull()).cast(BooleanType()), col("`CTS v3 CC`"))\
            .otherwise(col("`CTS v3`"))\
            .cast(StringType())
        )\
        .withColumn("CTS v3", when(((length(col("`CTS v3`")) == lit(0)) | col("`CTS v3`").isNull()).cast(BooleanType()), lit("Others"))\
        .otherwise(col("`CTS v3`"))\
        .cast(StringType()))
