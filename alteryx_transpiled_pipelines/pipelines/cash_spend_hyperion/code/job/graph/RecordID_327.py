from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def RecordID_327(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils import ProphecyDataFrame

    return ProphecyDataFrame(in0, spark).zipWithIndex(1, 1, "RecordID", spark)
