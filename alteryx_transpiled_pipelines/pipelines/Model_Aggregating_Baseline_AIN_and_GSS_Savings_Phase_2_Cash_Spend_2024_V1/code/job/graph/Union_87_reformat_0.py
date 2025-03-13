from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_87_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_AIN EW_Reduction`").cast(DoubleType()).alias("_AIN EW_Reduction"), 
        col("`_AIN EW_Reduction (Realised)`").cast(DoubleType()).alias("_AIN EW_Reduction (Realised)"), 
        col("`_AIN Ext. Lab. Net Savings Stage`").cast(StringType()).alias("_AIN Ext. Lab. Net Savings Stage"), 
        col("_AIN_Resource_Cost").cast(DoubleType()).alias("_AIN_Resource_Cost"), 
        col("`_AIN_Resource_Cost (Realised)`").cast(DoubleType()).alias("_AIN_Resource_Cost (Realised)"), 
        col("_AIN_Savings").cast(DoubleType()).alias("_AIN_Savings"), 
        col("`_AIN_Savings (Realised)`").cast(DoubleType()).alias("_AIN_Savings (Realised)"), 
        col("_ASHB").cast(StringType()).alias("_ASHB"), 
        col("`_CTS V3`").cast(StringType()).alias("_CTS V3"), 
        col("`_Cash Spend`").alias("_Cash Spend"), 
        col("_Category"), 
        col("`_Company Code`").cast(StringType()).alias("_Company Code"), 
        col("`_Company Description`").alias("_Company Description"), 
        col("`_Company Group`").alias("_Company Group"), 
        col("`_GSS BAU Savings`").cast(DoubleType()).alias("_GSS BAU Savings"), 
        col("`_GSS BAU Savings (Realised)`").cast(DoubleType()).alias("_GSS BAU Savings (Realised)"), 
        col("`_GSS Enterprise Savings`").cast(StringType()).alias("_GSS Enterprise Savings"), 
        col("`_GSS Incremental High Savings`").cast(DoubleType()).alias("_GSS Incremental High Savings"), 
        col("`_GSS Incremental High Savings (Realised)`")\
          .cast(StringType())\
          .alias("_GSS Incremental High Savings (Realised)"), 
        col("`_GSS Incremental Low Savings`").cast(DoubleType()).alias("_GSS Incremental Low Savings"), 
        col("`_GSS Incremental Low Savings (Realised)`")\
          .cast(DoubleType())\
          .alias("_GSS Incremental Low Savings (Realised)"), 
        col("`_Hyperion Category`").alias("_Hyperion Category"), 
        col("`_Hyperion Category Code`").alias("_Hyperion Category Code"), 
        col("`_In-Scope`").cast(StringType()).alias("_In-Scope"), 
        col("`_In-Scope Final`").alias("_In-Scope Final"), 
        col("`_Non-Opex Adjustments`").cast(DoubleType()).alias("_Non-Opex Adjustments"), 
        col("`_OSE Labor V2`").cast(StringType()).alias("_OSE Labor V2"), 
        col("`_Planning Account`").alias("_Planning Account"), 
        col("`_Planning Account Actual`").cast(StringType()).alias("_Planning Account Actual"), 
        col("`_Planning Account Description`").alias("_Planning Account Description"), 
        col("`_Planning Account Description Actual`").cast(StringType()).alias("_Planning Account Description Actual"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("_Sum_Adjusted_Sum_LCL").cast(StringType()).alias("_Sum_Adjusted_Sum_LCL"), 
        col("_Sum_Adjusted_Sum_LCLCFX"), 
        col("_Sum_Adjusted_Sum_USDAFX").cast(StringType()).alias("_Sum_Adjusted_Sum_USDAFX"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("_evp"), 
        col("`_gl account`").alias("_gl account"), 
        col("`_gl account number`").alias("_gl account number"), 
        col("_hlmc"), 
        col("`_material group`").alias("_material group"), 
        col("`_material group description`").alias("_material group description"), 
        col("_mc"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`_product description`").alias("_product description"), 
        col("_quarter"), 
        col("`_scenario name`").alias("_scenario name"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("_site"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("`_tableau display category`").cast(StringType()).alias("_tableau display category"), 
        col("_version"), 
        col("_year")
    )
