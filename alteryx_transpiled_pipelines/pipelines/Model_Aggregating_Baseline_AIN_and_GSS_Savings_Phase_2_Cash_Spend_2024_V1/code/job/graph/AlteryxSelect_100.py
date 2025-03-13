from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_100(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`In-Scope Final`").alias("In-Scope Final"), 
        col("mc"), 
        col("`_GSS Incremental High Savings`").alias("_GSS Incremental High Savings"), 
        col("ASHB"), 
        col("`_AIN EW_Reduction`").alias("_AIN EW_Reduction"), 
        col("`Company Description`").alias("Company Description"), 
        col("`gl account number`").alias("gl account number"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`Cash Spend`").alias("Cash Spend"), 
        col("Site"), 
        col("`_GSS BAU Savings`").alias("_GSS BAU Savings"), 
        col("`company code`").alias("company code"), 
        col("`_GSS Incremental Low Savings`").alias("_GSS Incremental Low Savings"), 
        col("`scenario name`").alias("scenario name"), 
        col("`Company Group`").alias("Company Group"), 
        col("hlmc"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`OSE Labor V2`").alias("OSE Labor V2"), 
        col("`_Hyperion Category Code`").alias("_Hyperion Category Code"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("_version"), 
        col("`cost center number`").alias("cost center number"), 
        col("`_product description`").alias("_product description"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("_Sum_Adjusted_Sum_USDAFX"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("`_Hyperion Category`").alias("_Hyperion Category"), 
        col("_AIN_Resource_Cost"), 
        col("_Sum_Adjusted_Sum_LCL"), 
        col("quarter"), 
        col("`Planning Account Description`").alias("Planning Account Description"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("_AIN_Savings"), 
        col("category"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`material group description`").alias("material group description")
    )
