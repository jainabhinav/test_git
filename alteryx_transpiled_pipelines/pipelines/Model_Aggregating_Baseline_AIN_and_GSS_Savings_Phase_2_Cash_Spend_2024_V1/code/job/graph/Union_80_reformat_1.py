from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_80_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_%Split").cast(DoubleType()).alias("_%Split"), 
        col("`_AIN EW_Reduction`").cast(DoubleType()).alias("_AIN EW_Reduction"), 
        col("`_AIN EW_Reduction (Realised)`").cast(DoubleType()).alias("_AIN EW_Reduction (Realised)"), 
        col("`_AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        col("_AIN_Resource_Cost").cast(DoubleType()).alias("_AIN_Resource_Cost"), 
        col("`_AIN_Resource_Cost (Realised)`").cast(DoubleType()).alias("_AIN_Resource_Cost (Realised)"), 
        col("_AIN_Savings").cast(DoubleType()).alias("_AIN_Savings"), 
        col("`_AIN_Savings (Realised)`").cast(DoubleType()).alias("_AIN_Savings (Realised)"), 
        col("_ASHB").cast(StringType()).alias("_ASHB"), 
        col("`_CTS v3`").cast(StringType()).alias("_CTS v3"), 
        col("`_Cash Spend`").cast(DoubleType()).alias("_Cash Spend"), 
        col("_Category").cast(StringType()).alias("_Category"), 
        col("`_Company Code`").cast(DoubleType()).alias("_Company Code"), 
        col("`_Company Description`").cast(StringType()).alias("_Company Description"), 
        col("`_GSS BAU Savings`").cast(DoubleType()).alias("_GSS BAU Savings"), 
        col("`_GSS BAU Savings (Realised)`").cast(DoubleType()).alias("_GSS BAU Savings (Realised)"), 
        col("`_GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        col("`_GSS Incremental High Savings`").cast(DoubleType()).alias("_GSS Incremental High Savings"), 
        col("`_GSS Incremental High Savings (Realised)`")\
          .cast(StringType())\
          .alias("_GSS Incremental High Savings (Realised)"), 
        col("`_GSS Incremental Low Savings`").cast(DoubleType()).alias("_GSS Incremental Low Savings"), 
        col("`_GSS Incremental Low Savings (Realised)`")\
          .cast(DoubleType())\
          .alias("_GSS Incremental Low Savings (Realised)"), 
        col("`_Hyperion Category`").cast(StringType()).alias("_Hyperion Category"), 
        col("`_Hyperion Category Code`").cast(StringType()).alias("_Hyperion Category Code"), 
        col("`_In-Scope`").alias("_In-Scope"), 
        col("`_Non-Opex Adjustments`").cast(DoubleType()).alias("_Non-Opex Adjustments"), 
        col("`_OSE Labor`").cast(StringType()).alias("_OSE Labor"), 
        col("`_Planning Account`").cast(StringType()).alias("_Planning Account"), 
        col("`_Planning Account Actual`").cast(StringType()).alias("_Planning Account Actual"), 
        col("`_Planning Account Description`").cast(StringType()).alias("_Planning Account Description"), 
        col("`_Planning Account Description Actual`").cast(StringType()).alias("_Planning Account Description Actual"), 
        col("_Sum_Adjusted_Sum_LCL").cast(DoubleType()).alias("_Sum_Adjusted_Sum_LCL"), 
        col("_Sum_Adjusted_Sum_LCLCFX").cast(DoubleType()).alias("_Sum_Adjusted_Sum_LCLCFX"), 
        col("_Sum_Adjusted_Sum_USDAFX").cast(DoubleType()).alias("_Sum_Adjusted_Sum_USDAFX"), 
        col("`_cost center`").cast(StringType()).alias("_cost center"), 
        col("`_cost center number`").cast(StringType()).alias("_cost center number"), 
        col("_evp").cast(StringType()).alias("_evp"), 
        col("`_gl account`").cast(StringType()).alias("_gl account"), 
        col("`_gl account number`").cast(StringType()).alias("_gl account number"), 
        col("_hlmc").cast(StringType()).alias("_hlmc"), 
        col("`_material group`").cast(StringType()).alias("_material group"), 
        col("`_material group description`").cast(StringType()).alias("_material group description"), 
        col("_mc").cast(StringType()).alias("_mc"), 
        col("`_planning sku`").cast(StringType()).alias("_planning sku"), 
        col("`_product description`").cast(StringType()).alias("_product description"), 
        col("_quarter").cast(StringType()).alias("_quarter"), 
        col("`_regrouped level 4`").cast(StringType()).alias("_regrouped level 4"), 
        col("`_scenario name`").cast(StringType()).alias("_scenario name"), 
        col("`_scenario type`").cast(StringType()).alias("_scenario type"), 
        col("_site").cast(StringType()).alias("_site"), 
        col("`_sub mc`").cast(StringType()).alias("_sub mc"), 
        col("`_tableau display category`").cast(StringType()).alias("_tableau display category"), 
        col("`_tableau display mega category`").cast(StringType()).alias("_tableau display mega category"), 
        col("_version").cast(StringType()).alias("_version"), 
        col("_year").cast(StringType()).alias("_year")
    )
