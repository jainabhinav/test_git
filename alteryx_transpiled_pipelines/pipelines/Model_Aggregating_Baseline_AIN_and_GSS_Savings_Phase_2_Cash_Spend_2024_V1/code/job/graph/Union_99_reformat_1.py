from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_99_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(DoubleType()).alias("_AIN EW_Reduction"), 
        col("`_AIN EW_Reduction (Realised)`").alias("_AIN EW_Reduction (Realised)"), 
        col("`_AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        lit(None).cast(DoubleType()).alias("_AIN_Resource_Cost"), 
        col("`_AIN_Resource_Cost (Realised)`").alias("_AIN_Resource_Cost (Realised)"), 
        lit(None).cast(DoubleType()).alias("_AIN_Savings"), 
        col("`_AIN_Savings (Realised)`").alias("_AIN_Savings (Realised)"), 
        col("_ASHB"), 
        col("`_CTS V3`").alias("_CTS V3"), 
        col("`_Cash Spend`").cast(StringType()).alias("_Cash Spend"), 
        col("_Category"), 
        col("`_Company Code`").cast(StringType()).alias("_Company Code"), 
        col("`_Company Description`").alias("_Company Description"), 
        col("`_Company Group`").alias("_Company Group"), 
        lit(None).cast(DoubleType()).alias("_GSS BAU Savings"), 
        col("`_GSS BAU Savings (Realised)`").alias("_GSS BAU Savings (Realised)"), 
        col("`_GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        lit(None).cast(DoubleType()).alias("_GSS Incremental High Savings"), 
        col("`_GSS Incremental High Savings (Realised)`").alias("_GSS Incremental High Savings (Realised)"), 
        lit(None).cast(DoubleType()).alias("_GSS Incremental Low Savings"), 
        col("`_GSS Incremental Low Savings (Realised)`").alias("_GSS Incremental Low Savings (Realised)"), 
        lit(None).cast(StringType()).alias("_Hyperion Category"), 
        lit(None).cast(StringType()).alias("_Hyperion Category Code"), 
        col("`_In-Scope`").alias("_In-Scope"), 
        col("`_In-Scope Final`").alias("_In-Scope Final"), 
        col("`_Non-Opex Adjustments`").cast(StringType()).alias("_Non-Opex Adjustments"), 
        col("`_OSE Labor V2`").alias("_OSE Labor V2"), 
        col("`_Planning Account`").alias("_Planning Account"), 
        col("`_Planning Account Actual`").alias("_Planning Account Actual"), 
        col("`_Planning Account Description`").alias("_Planning Account Description"), 
        col("`_Planning Account Description Actual`").alias("_Planning Account Description Actual"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        lit(None).cast(DoubleType()).alias("_Sum_Adjusted_Sum_LCL"), 
        col("_Sum_Adjusted_Sum_LCLCFX").cast(StringType()).alias("_Sum_Adjusted_Sum_LCLCFX"), 
        lit(None).cast(DoubleType()).alias("_Sum_Adjusted_Sum_USDAFX"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("_evp"), 
        col("`_gl account`").alias("_gl account"), 
        col("`_gl account number`").alias("_gl account number"), 
        col("_hlmc"), 
        col("`_material group`").alias("_material group"), 
        col("`_material group description`").alias("_material group description"), 
        col("_mc"), 
        lit(None).cast(StringType()).alias("_planning sku"), 
        lit(None).cast(StringType()).alias("_product description"), 
        col("_quarter"), 
        col("`_scenario name`").alias("_scenario name"), 
        lit(None).cast(StringType()).alias("_scenario type"), 
        col("_site"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("`_tableau display category`").alias("_tableau display category"), 
        lit(None).cast(StringType()).alias("_version"), 
        col("_year")
    )
