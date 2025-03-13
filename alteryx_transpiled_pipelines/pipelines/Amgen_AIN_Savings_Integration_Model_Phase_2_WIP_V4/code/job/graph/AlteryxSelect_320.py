from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_320(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`OSE Labor V2`").alias("OSE Labor V2 Mapped in Data"), 
        col("ASHB").alias("ASHB Mapped in Data"), 
        col("year"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS Mapped in Data")
    )
