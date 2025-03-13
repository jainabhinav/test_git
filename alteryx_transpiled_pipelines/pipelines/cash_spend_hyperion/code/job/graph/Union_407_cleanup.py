from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_407_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_Exclusion Type`").alias("Exclusion Type"), 
        col("`_pa account desc`").alias("pa account desc"), 
        col("`_sub mc2`").alias("sub mc2"), 
        col("`_acct level 5`").alias("acct level 5"), 
        col("`_site detail`").alias("site detail"), 
        col("_LCL").alias("LCL"), 
        col("`_scenario name`").alias("scenario name"), 
        col("`_company code`").alias("company code"), 
        col("`_sub mc`").alias("sub mc"), 
        col("_site").alias("site"), 
        col("`_local currency`").alias("local currency"), 
        col("_year").alias("year"), 
        col("_LCLCFX").alias("LCLCFX"), 
        col("`_category code`").alias("category code"), 
        col("`_material group`").alias("material group"), 
        col("`_acct level 3`").alias("acct level 3"), 
        col("_quarter").alias("quarter"), 
        col("_company").alias("company"), 
        col("_version").alias("version"), 
        col("`_scenario type`").alias("scenario type"), 
        col("`_planning sku`").alias("planning sku"), 
        col("_site_nonsite").alias("site_nonsite"), 
        col("`_cost center number`").alias("cost center number"), 
        col("`_gl account`").alias("gl account"), 
        col("_evp").alias("evp"), 
        col("`_pa account`").alias("pa account"), 
        col("_category").alias("category"), 
        col("`_acct level 4`").alias("acct level 4"), 
        col("_mc").alias("mc"), 
        col("_hlmc").alias("hlmc"), 
        col("`_regrouped level 4`").alias("regrouped level 4"), 
        col("`_cost center`").alias("cost center"), 
        col("`_product description`").alias("product description"), 
        col("_USDAFX").alias("USDAFX"), 
        col("`_gl account number`").alias("gl account number")
    )
