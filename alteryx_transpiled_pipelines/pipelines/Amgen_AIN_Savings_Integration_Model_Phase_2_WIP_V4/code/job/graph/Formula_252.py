from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_252(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("AIN_Savings", (col("AIN_Savings") * col("`DTI % Split`")).cast(DoubleType()))\
        .withColumn("AIN_Resource_Cost", (col("AIN_Resource_Cost") * col("`DTI % Split`")).cast(DoubleType()))\
        .withColumn("EW_Reduction", (col("EW_Reduction") * col("`DTI % Split`")).cast(DoubleType()))
