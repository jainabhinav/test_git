from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_99_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Non-Opex Adjustments`").alias("_Non-Opex Adjustments"), 
        col("`CTS v3`").alias("_CTS V3"), 
        col("`In-Scope Final`").alias("_In-Scope Final"), 
        col("Sum_Adjusted_Sum_LCLCFX").alias("_Sum_Adjusted_Sum_LCLCFX"), 
        col("`Company Group`").alias("_Company Group"), 
        col("`Planning Account Description`").alias("_Planning Account Description"), 
        col("`Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("`In-Scope`").alias("_In-Scope"), 
        col("`GSS BAU Savings (Realised)`").alias("_GSS BAU Savings (Realised)"), 
        col("`GSS Enterprise Savings`").alias("_GSS Enterprise Savings"), 
        col("hlmc").alias("_hlmc"), 
        col("year").alias("_year"), 
        col("`AIN_Savings (Realised)`").alias("_AIN_Savings (Realised)"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("ASHB").alias("_ASHB"), 
        col("`tableau display category`").alias("_tableau display category"), 
        col("`Planning Account`").alias("_Planning Account"), 
        col("`company code`").alias("_Company Code"), 
        col("`AIN_Resource_Cost (Realised)`").alias("_AIN_Resource_Cost (Realised)"), 
        col("Site").alias("_site"), 
        col("mc").alias("_mc"), 
        col("`AIN Ext. Lab. Net Savings Stage`").alias("_AIN Ext. Lab. Net Savings Stage"), 
        col("`GSS Incremental Low Savings (Realised)`").alias("_GSS Incremental Low Savings (Realised)"), 
        col("`OSE Labor V2`").alias("_OSE Labor V2"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("`AIN EW_Reduction (Realised)`").alias("_AIN EW_Reduction (Realised)"), 
        col("`Company Description`").alias("_Company Description"), 
        col("`Planning Account Description Actual`").alias("_Planning Account Description Actual"), 
        col("`Planning Account Actual`").alias("_Planning Account Actual"), 
        col("`material group description`").alias("_material group description"), 
        col("quarter").alias("_quarter"), 
        col("`GSS Incremental High Savings (Realised)`").alias("_GSS Incremental High Savings (Realised)"), 
        col("evp").alias("_evp"), 
        col("`Cash Spend`").alias("_Cash Spend"), 
        col("category").alias("_Category")
    )
