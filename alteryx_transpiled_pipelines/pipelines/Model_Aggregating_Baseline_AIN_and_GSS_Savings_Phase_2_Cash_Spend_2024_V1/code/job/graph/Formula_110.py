from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_110(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`AIN EW_Reduction (Realised)`").alias("AIN EW_Reduction (Realised)"), 
        col("_version"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("ASHB"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("_AIN_Resource_Cost"), 
        col("`AIN_Resource_Cost (Realised)`").alias("AIN_Resource_Cost (Realised)"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`GSS Enterprise Savings`").alias("GSS Enterprise Savings"), 
        col("`sub mc`").alias("sub mc"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("`AIN Ext. Lab. Net Savings Stage`").alias("AIN Ext. Lab. Net Savings Stage"), 
        col("year"), 
        col("`_GSS BAU Savings`").alias("_GSS BAU Savings"), 
        col("`material group`").alias("material group"), 
        col("`GSS BAU Savings (Realised)`").alias("GSS BAU Savings (Realised)"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`GSS Incremental High Savings (Realised)`").alias("GSS Incremental High Savings (Realised)"), 
        col("quarter"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`_AIN EW_Reduction`").alias("_AIN EW_Reduction"), 
        col("`AIN_Savings (Realised)`").alias("AIN_Savings (Realised)"), 
        col("`_Hyperion Category`").alias("_Hyperion Category"), 
        col("`Company Group`").alias("Company Group"), 
        col("`_GSS Incremental Low Savings`").alias("_GSS Incremental Low Savings"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`cost center number`").alias("cost center number"), 
        col("_Sum_Adjusted_Sum_LCL"), 
        col("`gl account`").alias("gl account"), 
        col("`_product description`").alias("_product description"), 
        col("evp"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("category"), 
        col("`material group description`").alias("material group description"), 
        col("_AIN_Savings"), 
        col("mc"), 
        col("`In-Scope`").alias("In-Scope"), 
        col("hlmc"), 
        col("Site"), 
        col("`_GSS Incremental High Savings`").alias("_GSS Incremental High Savings"), 
        col("`GSS Incremental Low Savings (Realised)`").alias("GSS Incremental Low Savings (Realised)"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`In-Scope Final`").alias("In-Scope Final"), 
        col("`_Hyperion Category Code`").alias("_Hyperion Category Code"), 
        col("_Sum_Adjusted_Sum_USDAFX"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        when(lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), lit("PA Not Available (LRP)"))\
          .otherwise(col("`Planning Account`"))\
          .cast(StringType())\
          .alias("Planning Account Actual"), 
        when(
            lower(col("`scenario name`")).contains(lower(lit("LRP"))).cast(BooleanType()), 
            lit("PA Description Not Available (LRP)")
          )\
          .otherwise(col("`Planning Account Description`"))\
          .cast(StringType())\
          .alias("Planning Account Description Actual")
    )
