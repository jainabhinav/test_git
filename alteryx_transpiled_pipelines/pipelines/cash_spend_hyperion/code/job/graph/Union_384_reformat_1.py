from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_384_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_CTS v3`").alias("_CTS v3"), 
        col("_Description").cast(StringType()).alias("_Description"), 
        col("_Name").cast(StringType()).alias("_Name"), 
        col("`_Non-Opex Adjustments`").alias("_Non-Opex Adjustments"), 
        col("`_OSE Labor`").alias("_OSE Labor"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("_Right_GL").cast(StringType()).alias("_Right_GL"), 
        col("_Site"), 
        col("_Sum_Adjusted_Sum_LCLCFX"), 
        col("_category"), 
        col("_company"), 
        col("`_company code`").alias("_company code"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("_evp"), 
        col("`_gl account`").alias("_gl account"), 
        col("`_gl account number`").alias("_gl account number"), 
        col("_hlmc"), 
        col("`_material group`").alias("_material group"), 
        col("`_material group description`").alias("_material group description"), 
        col("_mc"), 
        col("`_pa account`").alias("_pa account"), 
        col("`_pa account desc`").alias("_pa account desc"), 
        col("_quarter"), 
        col("`_scenario name`").alias("_scenario name"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("_year")
    )
