from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_94(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`AIN EW_Reduction (Realised)`").alias("AIN EW_Reduction (Realised)"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("ASHB"), 
        col("Category"), 
        col("`Planning Account Actual`").alias("Planning Account Actual"), 
        col("`Planning Account Description Actual`").alias("Planning Account Description Actual"), 
        col("`AIN_Resource_Cost (Realised)`").alias("AIN_Resource_Cost (Realised)"), 
        col("`scenario name`").alias("scenario name"), 
        col("`GSS Enterprise Savings`").alias("GSS Enterprise Savings"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("`AIN Ext. Lab. Net Savings Stage`").alias("AIN Ext. Lab. Net Savings Stage"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`GSS BAU Savings (Realised)`").alias("GSS BAU Savings (Realised)"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`GSS Incremental High Savings (Realised)`").alias("GSS Incremental High Savings (Realised)"), 
        col("quarter"), 
        col("`Company Code`").alias("Company Code"), 
        col("`CTS V3`").alias("CTS V3"), 
        col("`AIN_Savings (Realised)`").alias("AIN_Savings (Realised)"), 
        col("`Hyperion Category`").alias("Hyperion Category"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`Hyperion Category Code`").alias("Hyperion Category Code"), 
        col("`Company Group`").alias("Company Group"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("`In-Scope`").alias("In-Scope"), 
        col("hlmc"), 
        col("`GSS Incremental Low Savings (Realised)`").alias("GSS Incremental Low Savings (Realised)"), 
        col("`cost center`").alias("cost center"), 
        col("`In-Scope Final`").alias("In-Scope Final"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number")
    )

    return df1.agg(
        sum(col("Sum_Adjusted_Sum_USDAFX")).alias("Sum_Adjusted_Sum_USDAFX"), 
        sum(col("Sum_Adjusted_Sum_LCLCFX")).alias("Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("Sum_Adjusted_Sum_LCL")).alias("Sum_Adjusted_Sum_LCL"), 
        sum(col("`Non-Opex Adjustments`")).alias("Non-Opex Adjustments"), 
        sum(col("`Cash Spend`")).alias("Cash Spend"), 
        sum(col("`GSS BAU Savings`")).alias("GSS BAU Savings"), 
        sum(col("`GSS Incremental Low Savings`")).alias("GSS Incremental Low Savings"), 
        sum(col("`GSS Incremental High Savings`")).alias("GSS Incremental High Savings"), 
        sum(col("AIN_Resource_Cost")).alias("AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("AIN_Savings"), 
        sum(col("`AIN EW_Reduction`")).alias("AIN EW_Reduction")
    )
