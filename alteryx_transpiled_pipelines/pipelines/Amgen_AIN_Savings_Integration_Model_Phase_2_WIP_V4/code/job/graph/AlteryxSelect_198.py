from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_198(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("`CTS Category`").alias("CTS Category"), col("`1`").alias("Planning Account"))
