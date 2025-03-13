from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_38_cast(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`AIN Ext. Lab. Net Savings Stage`").cast(StringType()).alias("AIN Ext. Lab. Net Savings Stage"), 
        col("`GSS Enterprise Savings`").cast(StringType()).alias("GSS Enterprise Savings"), 
        col("`In-Scope`").cast(StringType()).alias("In-Scope")
    )
