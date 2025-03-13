from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_331(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Cost Center Exists in Baseline?`").alias("Cost Center Exists in Baseline?"), 
        col("`OSE Labor V2 Mapped in Data`").alias("OSE Labor V2 Mapped in Data"), 
        col("`ASHB Mapped in Data`").alias("ASHB Mapped in Data"), 
        col("Year"), 
        col("`Hyperion CTS <> AIN Input CTS`").alias("Hyperion CTS <> AIN Input CTS"), 
        col("`OSE Labor V2 <> Labor?`").alias("OSE Labor V2 <> Labor?"), 
        col("`Cost Center Missing in Baseline?`").alias("Cost Center Missing in Baseline?"), 
        col("`ASHB = 0 or 'Not Found'?`").alias("ASHB = 0 or 'Not Found'?"), 
        col("Function"), 
        col("`CTS Mapped in Data`").alias("CTS Mapped in Data"), 
        col("EVP"), 
        col("HLMC"), 
        col("`Cost Center`").alias("Cost Center"), 
        col("Sum_AIN_Savings").alias("AIN Ext. Lab. Net Savings Leaked"), 
        col("Sum_EW_Reduction").alias("AIN Ext. Lab. Reduction Leaked")
    )
