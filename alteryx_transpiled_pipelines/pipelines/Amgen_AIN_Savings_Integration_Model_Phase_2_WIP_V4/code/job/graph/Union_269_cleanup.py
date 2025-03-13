from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_269_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_pa account desc`").alias("pa account desc"), 
        col("_AIN_Savings").alias("AIN_Savings"), 
        col("`_OSE Labor`").alias("OSE Labor"), 
        col("`_scenario name`").alias("scenario name"), 
        col("`_company code`").alias("company code"), 
        col("`_sub mc`").alias("sub mc"), 
        col("_site").alias("site"), 
        col("_Year").alias("Year"), 
        col("`_material group`").alias("material group"), 
        col("_ASHB").alias("ASHB"), 
        col("_quarter").alias("quarter"), 
        col("`_hyperion category code`").alias("hyperion category code"), 
        col("_company").alias("company"), 
        col("_version").alias("version"), 
        col("`_scenario type`").alias("scenario type"), 
        col("`_planning sku`").alias("planning sku"), 
        col("_EW_Reduction").alias("EW_Reduction"), 
        col("`_cost center number`").alias("cost center number"), 
        col("`_gl account`").alias("gl account"), 
        col("`_hyperion category`").alias("hyperion category"), 
        col("_EVP").alias("EVP"), 
        col("`_CTS v3`").alias("CTS v3"), 
        col("`_pa account`").alias("pa account"), 
        col("_Category").alias("Category"), 
        col("`_material group description`").alias("material group description"), 
        col("_mc").alias("mc"), 
        col("_HLMC").alias("HLMC"), 
        col("_AIN_Resource_Cost").alias("AIN_Resource_Cost"), 
        col("`_regrouped level 4`").alias("regrouped level 4"), 
        col("`_cost center`").alias("cost center"), 
        col("`_product description`").alias("product description"), 
        col("_Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`_gl account number`").alias("gl account number")
    )
