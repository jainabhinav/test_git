from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_88(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        col("mc"), 
        col("`planning sku`").alias("planning sku"), 
        col("`_GSS Incremental High Savings`").alias("_GSS Incremental High Savings"), 
        col("`_AIN EW_Reduction`").alias("_AIN EW_Reduction"), 
        col("`_In-Scope`").alias("_In-Scope"), 
        col("`_CTS V3`").alias("_CTS V3"), 
        col("`_GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        col("`gl account number`").alias("gl account number"), 
        col("`_Planning Account Description Actual`").alias("_Planning Account Description Actual"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`_AIN EW_Reduction (Realised)`").alias("_AIN EW_Reduction (Realised)"), 
        col("`_AIN_Savings (Realised)`").alias("_AIN_Savings (Realised)"), 
        col("_ASHB"), 
        col("`_GSS BAU Savings`").alias("_GSS BAU Savings"), 
        col("`_GSS Incremental Low Savings`").alias("_GSS Incremental Low Savings"), 
        col("`product description`").alias("product description"), 
        col("`_GSS BAU Savings (Realised)`").alias("_GSS BAU Savings (Realised)"), 
        col("`scenario name`").alias("scenario name"), 
        col("`_GSS Incremental High Savings (Realised)`").alias("_GSS Incremental High Savings (Realised)"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("version"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`_tableau display category`").alias("_tableau display category"), 
        col("`cost center`").alias("cost center"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("site"), 
        col("`_Planning Account Actual`").alias("_Planning Account Actual"), 
        col("_AIN_Resource_Cost"), 
        col("`_GSS Incremental Low Savings (Realised)`").alias("_GSS Incremental Low Savings (Realised)"), 
        col("quarter"), 
        col("`_AIN_Resource_Cost (Realised)`").alias("_AIN_Resource_Cost (Realised)"), 
        col("_AIN_Savings"), 
        col("`_Non-Opex Adjustments`").alias("_Non-Opex Adjustments"), 
        col("`_OSE Labor V2`").alias("_OSE Labor V2"), 
        col("`material group description`").alias("material group description"), 
        col("Sum_USDAFX").alias("Sum_Adjusted_Sum_USDAFX"), 
        col("`pa account`").alias("Planning Account"), 
        col("Sum_LCLCFX").cast(DoubleType()).alias("Sum_Adjusted_Sum_LCLCFX"), 
        col("Sum_LCL").alias("Sum_Adjusted_Sum_LCL"), 
        col("category").alias("Hyperion Category"), 
        col("`category code`").alias("Hyperion Category Code"), 
        col("`pa account desc`").alias("Planning Account Description"), 
        col("company").alias("Company Description"), 
        col("`company code`").cast(DoubleType()).alias("Company Code"), 
        col("`Exclusion Type`").alias("In-Scope Final")
    )
