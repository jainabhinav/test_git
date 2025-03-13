from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_51(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("DRM_GrandParent_Descr").alias("Hyp Cat"), 
        col("`Parent Node`").alias("PA Account"), 
        col("`Parent Description`").alias("PA Account Description")
    )
