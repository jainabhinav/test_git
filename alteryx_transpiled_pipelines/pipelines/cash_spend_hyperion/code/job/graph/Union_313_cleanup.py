from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_313_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_pa account desc`").alias("pa account desc"), 
        col("`_scenario name`").alias("scenario name"), 
        col("`_company code`").alias("company code"), 
        col("`_sub mc`").alias("sub mc"), 
        col("_site").alias("site"), 
        col("`_Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("_year").alias("year"), 
        col("`_material group`").alias("material group"), 
        col("_quarter").alias("quarter"), 
        col("`_hyperion category code`").alias("hyperion category code"), 
        col("_company").alias("company"), 
        col("_version").alias("version"), 
        col("`_scenario type`").alias("scenario type"), 
        col("`_planning sku`").alias("planning sku"), 
        col("_Sum_Adjusted_Sum_USDAFX").alias("Sum_Adjusted_Sum_USDAFX"), 
        col("`_cost center number`").alias("cost center number"), 
        col("`_gl account`").alias("gl account"), 
        col("`_% Split`").alias("% Split"), 
        col("`_hyperion category`").alias("hyperion category"), 
        col("_evp").alias("evp"), 
        col("`_pa account`").alias("pa account"), 
        col("_Sum_Adjusted_Sum_LCL").alias("Sum_Adjusted_Sum_LCL"), 
        col("_Category").alias("Category"), 
        col("_mc").alias("mc"), 
        col("_hlmc").alias("hlmc"), 
        col("`_regrouped level 4`").alias("regrouped level 4"), 
        col("`_cost center`").alias("cost center"), 
        col("`_product description`").alias("product description"), 
        col("_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Adjusted_Sum_LCLCFX"), 
        col("`_gl account number`").alias("gl account number")
    )
