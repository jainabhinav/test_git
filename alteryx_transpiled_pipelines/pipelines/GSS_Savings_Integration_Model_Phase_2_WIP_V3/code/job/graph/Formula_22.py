from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_22(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("%spilt"), 
        col("`Right_Mega Category`").alias("Right_Mega Category"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Incremental Savings High`").alias("Incremental Savings High"), 
        col("Right_EVP"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Incremental Savings Low Split`").alias("Incremental Savings Low Split"), 
        col("ASHB"), 
        col("Category"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("`scenario name`").alias("scenario name"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("`Mega Category`").alias("Mega Category"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("Right_Right_Year"), 
        col("quarter"), 
        col("`Company Code`").alias("Company Code"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`BAU Savings Split`").alias("BAU Savings Split"), 
        col("`planning sku`").alias("planning sku"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`cost center number`").alias("cost center number"), 
        col("`Incremental Savings High Split`").alias("Incremental Savings High Split"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`BAU Savings`").alias("BAU Savings"), 
        col("`material group description`").alias("material group description"), 
        col("Right_Year"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`Incremental Savings Low`").alias("Incremental Savings Low"), 
        col("`% Allocation - Incremental`").alias("% Allocation - Incremental"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("RecordID"), 
        col("`product description`").alias("product description"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        col("`% Allocation - BAU`").alias("% Allocation - BAU"), 
        (col("`BAU Savings Split`") * col("%spilt")).cast(DoubleType()).alias("GSS BAU Savings"), 
        (col("`Incremental Savings Low Split`") * col("%spilt")).cast(DoubleType()).alias("GSS Incremental Low Savings"), 
        (col("`Incremental Savings High Split`") * col("%spilt"))\
          .cast(DoubleType())\
          .alias("GSS Incremental High Savings")
    )
