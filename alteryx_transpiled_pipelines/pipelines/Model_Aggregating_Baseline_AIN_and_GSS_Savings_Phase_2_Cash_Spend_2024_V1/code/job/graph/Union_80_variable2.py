from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_80_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        col("`GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        col("`In-Scope`").alias("_In-Scope")
    )
