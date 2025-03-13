from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_269_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_AIN_Resource_Cost"), 
        col("_AIN_Savings"), 
        col("_ASHB"), 
        col("`_CTS v3`").alias("_CTS v3"), 
        col("_Category"), 
        col("_EVP"), 
        col("_EW_Reduction"), 
        col("_HLMC"), 
        col("`_OSE Labor`").alias("_OSE Labor"), 
        col("_Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("_Year"), 
        col("_company"), 
        col("`_company code`").alias("_company code"), 
        col("`_cost center`").alias("_cost center"), 
        col("`_cost center number`").alias("_cost center number"), 
        col("`_gl account`").alias("_gl account"), 
        col("`_gl account number`").alias("_gl account number"), 
        col("`_hyperion category`").alias("_hyperion category"), 
        col("`_hyperion category code`").alias("_hyperion category code"), 
        col("`_material group`").alias("_material group"), 
        col("`_material group description`").alias("_material group description"), 
        col("_mc"), 
        col("`_pa account`").alias("_pa account"), 
        col("`_pa account desc`").alias("_pa account desc"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`_product description`").alias("_product description"), 
        col("_quarter"), 
        col("`_regrouped level 4`").alias("_regrouped level 4"), 
        col("`_scenario name`").alias("_scenario name"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("_site"), 
        col("`_sub mc`").alias("_sub mc"), 
        col("_version")
    )
