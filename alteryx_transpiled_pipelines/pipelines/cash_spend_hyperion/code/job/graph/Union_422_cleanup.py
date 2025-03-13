from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_422_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Exclusion Type`").alias("Exclusion Type"), 
        col("_Sum_USDAFX").alias("Sum_USDAFX"), 
        col("`_pa account desc`").alias("pa account desc"), 
        col("_Sum_LCL").alias("Sum_LCL"), 
        col("`_scenario name`").alias("scenario name"), 
        col("`_company code`").alias("company code"), 
        col("`_sub mc`").alias("sub mc"), 
        col("_Site").alias("Site"), 
        col("_Sum_LCLCFX").alias("Sum_LCLCFX"), 
        col("_year").alias("year"), 
        col("`_category code`").alias("category code"), 
        col("`_material group`").alias("material group"), 
        col("_quarter").alias("quarter"), 
        col("_company").alias("company"), 
        col("_version").alias("version"), 
        col("`_scenario type`").alias("scenario type"), 
        col("`_planning sku`").alias("planning sku"), 
        col("`_cost center number`").alias("cost center number"), 
        col("`_gl account`").alias("gl account"), 
        col("_evp").alias("evp"), 
        col("`_pa account`").alias("pa account"), 
        col("_category").alias("category"), 
        col("`_material group description`").alias("material group description"), 
        col("_mc").alias("mc"), 
        col("_hlmc").alias("hlmc"), 
        col("`_Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`_cost center`").alias("cost center"), 
        col("`_product description`").alias("product description"), 
        col("`_gl account number`").alias("gl account number")
    )
