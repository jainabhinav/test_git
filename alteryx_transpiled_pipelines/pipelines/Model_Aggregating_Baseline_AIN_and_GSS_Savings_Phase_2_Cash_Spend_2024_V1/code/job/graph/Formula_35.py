from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_35(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("AIN_Savings"), 
        col("ASHB"), 
        col("Category"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("`scenario name`").alias("scenario name"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("AIN_Resource_Cost"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`AIN EW_Reduction`").alias("AIN EW_Reduction"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("`Company Code`").alias("Company Code"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`planning sku`").alias("planning sku"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`cost center number`").alias("cost center number"), 
        col("`GSS BAU Savings`").alias("GSS BAU Savings"), 
        col("`GSS Incremental High Savings`").alias("GSS Incremental High Savings"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`GSS Incremental Low Savings`").alias("GSS Incremental Low Savings"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        lit(0).cast(DoubleType()).alias("GSS BAU Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("GSS Incremental Low Savings (Realised)"), 
        lit(0).cast(StringType()).alias("GSS Incremental High Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN_Resource_Cost (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN_Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN EW_Reduction (Realised)"), 
        lit("Value Ambition").cast(StringType()).alias("AIN Ext. Lab. Net Savings Stage"), 
        lit("Value Targeting").cast(StringType()).alias("GSS Enterprise Savings"), 
        lit("Yes").cast(StringType()).alias("In-Scope")
    )
