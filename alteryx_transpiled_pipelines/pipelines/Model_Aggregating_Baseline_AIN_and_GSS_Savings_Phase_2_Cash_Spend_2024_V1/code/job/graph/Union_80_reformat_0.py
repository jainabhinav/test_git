from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_80_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(DoubleType()).alias("_%Split"), 
        lit(None).cast(DoubleType()).alias("_AIN EW_Reduction"), 
        lit(None).cast(DoubleType()).alias("_AIN EW_Reduction (Realised)"), 
        col("`_AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        lit(None).cast(DoubleType()).alias("_AIN_Resource_Cost"), 
        lit(None).cast(DoubleType()).alias("_AIN_Resource_Cost (Realised)"), 
        lit(None).cast(DoubleType()).alias("_AIN_Savings"), 
        lit(None).cast(DoubleType()).alias("_AIN_Savings (Realised)"), 
        lit(None).cast(StringType()).alias("_ASHB"), 
        lit(None).cast(StringType()).alias("_CTS v3"), 
        lit(None).cast(DoubleType()).alias("_Cash Spend"), 
        lit(None).cast(StringType()).alias("_Category"), 
        lit(None).cast(DoubleType()).alias("_Company Code"), 
        lit(None).cast(StringType()).alias("_Company Description"), 
        lit(None).cast(DoubleType()).alias("_GSS BAU Savings"), 
        lit(None).cast(DoubleType()).alias("_GSS BAU Savings (Realised)"), 
        col("`_GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        lit(None).cast(DoubleType()).alias("_GSS Incremental High Savings"), 
        lit(None).cast(StringType()).alias("_GSS Incremental High Savings (Realised)"), 
        lit(None).cast(DoubleType()).alias("_GSS Incremental Low Savings"), 
        lit(None).cast(DoubleType()).alias("_GSS Incremental Low Savings (Realised)"), 
        lit(None).cast(StringType()).alias("_Hyperion Category"), 
        lit(None).cast(StringType()).alias("_Hyperion Category Code"), 
        col("`_In-Scope`").alias("_In-Scope"), 
        lit(None).cast(DoubleType()).alias("_Non-Opex Adjustments"), 
        lit(None).cast(StringType()).alias("_OSE Labor"), 
        lit(None).cast(StringType()).alias("_Planning Account"), 
        lit(None).cast(StringType()).alias("_Planning Account Actual"), 
        lit(None).cast(StringType()).alias("_Planning Account Description"), 
        lit(None).cast(StringType()).alias("_Planning Account Description Actual"), 
        lit(None).cast(DoubleType()).alias("_Sum_Adjusted_Sum_LCL"), 
        lit(None).cast(DoubleType()).alias("_Sum_Adjusted_Sum_LCLCFX"), 
        lit(None).cast(DoubleType()).alias("_Sum_Adjusted_Sum_USDAFX"), 
        lit(None).cast(StringType()).alias("_cost center"), 
        lit(None).cast(StringType()).alias("_cost center number"), 
        lit(None).cast(StringType()).alias("_evp"), 
        lit(None).cast(StringType()).alias("_gl account"), 
        lit(None).cast(StringType()).alias("_gl account number"), 
        lit(None).cast(StringType()).alias("_hlmc"), 
        lit(None).cast(StringType()).alias("_material group"), 
        lit(None).cast(StringType()).alias("_material group description"), 
        lit(None).cast(StringType()).alias("_mc"), 
        lit(None).cast(StringType()).alias("_planning sku"), 
        lit(None).cast(StringType()).alias("_product description"), 
        lit(None).cast(StringType()).alias("_quarter"), 
        lit(None).cast(StringType()).alias("_regrouped level 4"), 
        lit(None).cast(StringType()).alias("_scenario name"), 
        lit(None).cast(StringType()).alias("_scenario type"), 
        lit(None).cast(StringType()).alias("_site"), 
        lit(None).cast(StringType()).alias("_sub mc"), 
        lit(None).cast(StringType()).alias("_tableau display category"), 
        lit(None).cast(StringType()).alias("_tableau display mega category"), 
        lit(None).cast(StringType()).alias("_version"), 
        lit(None).cast(StringType()).alias("_year")
    )
