from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Cleanse_84_after(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`GSS Incremental Low Savings`").alias("GSS Incremental Low Savings"), 
        col("mc"), 
        col("ASHB"), 
        col("`Company Description`").alias("Company Description"), 
        col("`Company Code`").alias("Company Code"), 
        col("`gl account number`").alias("gl account number"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`AIN_Resource_Cost (Realised)`").alias("AIN_Resource_Cost (Realised)"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("`GSS Enterprise Savings`").alias("GSS Enterprise Savings"), 
        col("`scenario name`").alias("scenario name"), 
        col("`Company Group`").alias("Company Group"), 
        col("`GSS Incremental Low Savings (Realised)`").alias("GSS Incremental Low Savings (Realised)"), 
        col("`CTS V3`").alias("CTS V3"), 
        col("`AIN EW_Reduction`").alias("AIN EW_Reduction"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("AIN_Resource_Cost"), 
        col("`GSS Incremental High Savings (Realised)`").alias("GSS Incremental High Savings (Realised)"), 
        col("version"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("`AIN_Savings (Realised)`").alias("AIN_Savings (Realised)"), 
        col("`GSS Incremental High Savings`").alias("GSS Incremental High Savings"), 
        col("`AIN EW_Reduction (Realised)`").alias("AIN EW_Reduction (Realised)"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`cost center`").alias("cost center"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("Category"), 
        col("site"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("AIN_Savings"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`GSS BAU Savings (Realised)`").alias("GSS BAU Savings (Realised)"), 
        col("quarter"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`GSS BAU Savings`").alias("GSS BAU Savings"), 
        col("`material group description`").alias("material group description"), 
        col("In__Scope").alias("In-Scope"), 
        col("`Non__Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`In__Scope Final`").alias("In-Scope Final")
    )
