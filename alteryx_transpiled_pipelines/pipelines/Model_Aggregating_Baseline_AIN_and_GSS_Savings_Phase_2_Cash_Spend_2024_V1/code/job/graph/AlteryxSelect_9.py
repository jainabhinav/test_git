from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_9(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("mc"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`planning sku`").alias("planning sku"), 
        col("ASHB"), 
        col("`Company Description`").alias("Company Description"), 
        col("`Company Code`").alias("Company Code"), 
        col("`gl account number`").alias("gl account number"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("`product description`").alias("product description"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("version"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Category"), 
        col("site"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("quarter"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("RecordID"), 
        col("`material group description`").alias("material group description"), 
        col("Sum_Adjusted_Sum_LCLCFX").cast(DoubleType()).alias("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Non-Opex Adjustments`").cast(DoubleType()).alias("Non-Opex Adjustments"), 
        col("`Cash Spend`").cast(DoubleType()).alias("Cash Spend"), 
        col("AIN_Resource_Cost").cast(DoubleType()).alias("AIN_Resource_Cost"), 
        col("AIN_Savings").cast(DoubleType()).alias("AIN_Savings"), 
        col("EW_Reduction").cast(DoubleType()).alias("EW_Reduction")
    )
