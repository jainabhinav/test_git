from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_407_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("LCL").alias("_LCL"), 
        col("version").alias("_version"), 
        col("`planning sku`").alias("_planning sku"), 
        col("`regrouped level 4`").alias("_regrouped level 4"), 
        col("`acct level 3`").alias("_acct level 3"), 
        col("`site detail`").alias("_site detail"), 
        col("`sub mc2`").alias("_sub mc2"), 
        col("company").alias("_company"), 
        col("hlmc").alias("_hlmc"), 
        col("year").alias("_year"), 
        col("`pa account`").alias("_pa account"), 
        col("site_nonsite").alias("_site_nonsite"), 
        col("`scenario type`").alias("_scenario type"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("`acct level 4`").alias("_acct level 4"), 
        col("`Exclusion Type`").alias("_Exclusion Type"), 
        col("`company code`").alias("_company code"), 
        col("site").alias("_site"), 
        col("mc").alias("_mc"), 
        col("LCLCFX").alias("_LCLCFX"), 
        col("`local currency`").alias("_local currency"), 
        col("`category code`").alias("_category code"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`acct level 5`").alias("_acct level 5"), 
        col("`product description`").alias("_product description"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("quarter").alias("_quarter"), 
        col("USDAFX").alias("_USDAFX"), 
        col("`pa account desc`").alias("_pa account desc"), 
        col("evp").alias("_evp"), 
        col("category").alias("_category")
    )
