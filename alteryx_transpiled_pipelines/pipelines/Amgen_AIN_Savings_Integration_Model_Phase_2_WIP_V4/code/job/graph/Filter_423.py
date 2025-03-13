from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_423(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX") >= lit("0")))
