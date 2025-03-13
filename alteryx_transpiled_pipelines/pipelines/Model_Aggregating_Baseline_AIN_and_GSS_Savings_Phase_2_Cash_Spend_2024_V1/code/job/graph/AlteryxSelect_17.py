from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_17(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`GSS Incremental Low Savings`").alias("GSS Incremental Low Savings"), 
        col("mc"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`planning sku`").alias("planning sku"), 
        col("ASHB"), 
        col("`Right_scenario type`").alias("Right_scenario type"), 
        col("`Company Description`").alias("Company Description"), 
        col("`Company Code`").alias("Company Code"), 
        col("`Right_material group description`").alias("Right_material group description"), 
        col("`gl account number`").alias("gl account number"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("`Right_planning sku`").alias("Right_planning sku"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("Right_version"), 
        col("`Right_gl account number`").alias("Right_gl account number"), 
        col("`product description`").alias("product description"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("`Right_gl account`").alias("Right_gl account"), 
        col("version"), 
        col("`Right_sub mc`").alias("Right_sub mc"), 
        col("`GSS Incremental High Savings`").alias("GSS Incremental High Savings"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`Right_product description`").alias("Right_product description"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("Category"), 
        col("site"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`Right_OSE Labor`").alias("Right_OSE Labor"), 
        col("Right_ASHB"), 
        col("quarter"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("RecordID"), 
        col("`Right_material group`").alias("Right_material group"), 
        col("`GSS BAU Savings`").alias("GSS BAU Savings"), 
        col("`material group description`").alias("material group description")
    )
