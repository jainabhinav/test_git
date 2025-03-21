from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_12(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`GSS BAU Savings`").alias("GSS BAU Savings"), 
        col("AIN_Savings"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`scenario name`").alias("scenario name"), 
        col("`Company Code`").alias("Company Code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("`Non-Opex Adjustments`").cast(DoubleType()).alias("Non-Opex Adjustments"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`Company Description`").alias("Company Description"), 
        col("ASHB"), 
        col("quarter"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`cost center number`").alias("cost center number"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`GSS Incremental Low Savings`").alias("GSS Incremental Low Savings"), 
        col("`gl account`").alias("gl account"), 
        col("EW_Reduction").alias("AIN EW_Reduction"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("evp"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("Category"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("AIN_Resource_Cost"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`GSS Incremental High Savings`").alias("GSS Incremental High Savings"), 
        col("Sum_Adjusted_Sum_LCLCFX").cast(DoubleType()).alias("Sum_Adjusted_Sum_LCLCFX"), 
        col("`gl account number`").alias("gl account number")
    )
