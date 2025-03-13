from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_31_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(StringType()).alias("% Allocation - BAU"), 
        lit(None).cast(StringType()).alias("% Allocation - Incremental"), 
        lit(None).cast(DoubleType()).alias("%spilt"), 
        col("ASHB"), 
        lit(None).cast(DoubleType()).alias("BAU Savings"), 
        lit(None).cast(DoubleType()).alias("BAU Savings Split"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("Category"), 
        col("`Company Code`").alias("Company Code"), 
        col("`Company Description`").alias("Company Description"), 
        lit(None).cast(DoubleType()).alias("GSS BAU Savings"), 
        lit(None).cast(DoubleType()).alias("GSS Incremental High Savings"), 
        lit(None).cast(DoubleType()).alias("GSS Incremental Low Savings"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        lit(None).cast(DoubleType()).alias("Incremental Savings High"), 
        lit(None).cast(DoubleType()).alias("Incremental Savings High Split"), 
        lit(None).cast(DoubleType()).alias("Incremental Savings Low"), 
        lit(None).cast(DoubleType()).alias("Incremental Savings Low Split"), 
        lit(None).cast(StringType()).alias("Mega Category"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("RecordID"), 
        lit(None).cast(StringType()).alias("Right_EVP"), 
        lit(None).cast(StringType()).alias("Right_Mega Category"), 
        lit(None).cast(StringType()).alias("Right_Right_Year"), 
        lit(None).cast(StringType()).alias("Right_Year"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`cost center`").alias("cost center"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("`gl account`").alias("gl account"), 
        col("`gl account number`").alias("gl account number"), 
        col("hlmc"), 
        col("`material group`").alias("material group"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("`planning sku`").alias("planning sku"), 
        col("`product description`").alias("product description"), 
        col("quarter"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`scenario name`").alias("scenario name"), 
        col("`scenario type`").alias("scenario type"), 
        col("site"), 
        col("`sub mc`").alias("sub mc"), 
        col("version"), 
        col("year")
    )
