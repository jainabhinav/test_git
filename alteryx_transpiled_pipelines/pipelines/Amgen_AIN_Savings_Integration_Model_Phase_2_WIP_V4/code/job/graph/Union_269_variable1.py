from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_269_variable1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`CTS v3`").alias("_CTS v3"), 
        col("version").alias("_version"), 
        col("`planning sku`").alias("_planning sku"), 
        col("`regrouped level 4`").alias("_regrouped level 4"), 
        col("company").alias("_company"), 
        col("hlmc").alias("_HLMC"), 
        col("Year").alias("_Year"), 
        col("`pa account`").alias("_pa account"), 
        col("`scenario type`").alias("_scenario type"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("ASHB").alias("_ASHB"), 
        col("`company code`").alias("_company code"), 
        col("site").alias("_site"), 
        col("`OSE Labor`").alias("_OSE Labor"), 
        col("mc").alias("_mc"), 
        col("`hyperion category code`").alias("_hyperion category code"), 
        col("AIN_Savings").alias("_AIN_Savings"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`product description`").alias("_product description"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX").alias("_Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("`material group description`").alias("_material group description"), 
        col("quarter").alias("_quarter"), 
        col("EW_Reduction").alias("_EW_Reduction"), 
        col("`pa account desc`").alias("_pa account desc"), 
        col("`hyperion category`").alias("_hyperion category"), 
        col("evp").alias("_EVP"), 
        col("AIN_Resource_Cost").alias("_AIN_Resource_Cost"), 
        col("Category").alias("_Category")
    )
