from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_192(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("`OSE Labor V2`") == lit("Labor OSE")))
