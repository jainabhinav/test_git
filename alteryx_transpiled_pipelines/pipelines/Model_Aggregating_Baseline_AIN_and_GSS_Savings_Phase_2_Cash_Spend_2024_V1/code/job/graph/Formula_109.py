from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_109(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Cash Spend`").alias("Cash Spend"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("_version"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("ASHB"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("_AIN_Resource_Cost"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("year"), 
        col("`_GSS BAU Savings`").alias("_GSS BAU Savings"), 
        col("`material group`").alias("material group"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("quarter"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`_AIN EW_Reduction`").alias("_AIN EW_Reduction"), 
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
        col("hlmc"), 
        col("Site"), 
        col("`_GSS Incremental High Savings`").alias("_GSS Incremental High Savings"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`In-Scope Final`").alias("In-Scope Final"), 
        col("`_Hyperion Category Code`").alias("_Hyperion Category Code"), 
        col("_Sum_Adjusted_Sum_USDAFX"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        lit(0).cast(DoubleType()).alias("GSS BAU Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("GSS Incremental Low Savings (Realised)"), 
        lit(0).cast(StringType()).alias("GSS Incremental High Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN_Resource_Cost (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN_Savings (Realised)"), 
        lit(0).cast(DoubleType()).alias("AIN EW_Reduction (Realised)"), 
        lit("Value Ambition").cast(StringType()).alias("AIN Ext. Lab. Net Savings Stage"), 
        lit("Value Targeting").cast(StringType()).alias("GSS Enterprise Savings"), 
        lit("Yes").cast(StringType()).alias("In-Scope")
    )
