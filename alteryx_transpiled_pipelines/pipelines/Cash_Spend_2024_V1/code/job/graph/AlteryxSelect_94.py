from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_94(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").cast(DoubleType()).alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("Site"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("ASHB"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("quarter"), 
        col("company"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`pa account`").alias("pa account"), 
        col("category"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`cost center`").alias("cost center"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`gl account number`").alias("gl account number")
    )
