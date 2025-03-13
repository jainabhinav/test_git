from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_267(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("AIN_Savings"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("Year"), 
        col("`material group`").alias("material group"), 
        col("ASHB"), 
        col("quarter"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("EW_Reduction"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("EVP"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`pa account`").alias("pa account"), 
        col("Category"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("HLMC"), 
        col("AIN_Resource_Cost"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`Right_cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`gl account number`").alias("gl account number")
    )
