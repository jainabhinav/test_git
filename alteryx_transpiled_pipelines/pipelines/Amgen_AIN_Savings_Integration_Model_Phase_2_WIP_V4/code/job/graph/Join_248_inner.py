from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_248_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.Function") == col("in1.`CTS v3`")) & (col("in0.Year") == col("in1.year"))),
          "inner"
        )\
        .select(col("in0.Cumulative/Incremental").alias("Cumulative/Incremental"), col("in1.`DTI % Split`").alias("DTI % Split"), col("in1.`pa account desc`").alias("pa account desc"), col("in1.`Split Divide`").alias("Split Divide"), col("in0.AIN_Savings").alias("AIN_Savings"), col("in1.`OSE Labor`").alias("OSE Labor"), col("in1.Sum_Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"), col("in1.`scenario name`").alias("scenario name"), col("in1.`company code`").alias("company code"), col("in1.`sub mc`").alias("sub mc"), col("in1.site").alias("site"), col("in0.Year").alias("Year"), col("in1.`material group`").alias("material group"), col("in1.ASHB").alias("ASHB"), col("in1.quarter").alias("quarter"), col("in1.`hyperion category code`").alias("hyperion category code"), col("in1.company").alias("company"), col("in1.version").alias("version"), col("in1.`scenario type`").alias("scenario type"), col("in1.`planning sku`").alias("planning sku"), col("in0.EW_Reduction").alias("EW_Reduction"), col("in1.`cost center number`").alias("cost center number"), col("in0.Function").alias("Function"), col("in1.`gl account`").alias("gl account"), col("in1.`hyperion category`").alias("hyperion category"), col("in1.evp").alias("evp"), col("in1.`CTS v3`").alias("CTS v3"), col("in1.`pa account`").alias("pa account"), col("in1.Category").alias("Category"), col("in1.`material group description`").alias("material group description"), col("in1.mc").alias("mc"), col("in1.hlmc").alias("hlmc"), col("in0.AIN_Resource_Cost").alias("AIN_Resource_Cost"), col("in1.`regrouped level 4`").alias("regrouped level 4"), col("in1.`cost center`").alias("cost center"), col("in1.`product description`").alias("product description"), col("in1.year").alias("Right_year"), col("in1.Group").alias("Group"), col("in1.`Right_scenario name`").alias("Right_scenario name"), col("in1.Sum_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Sum_Adjusted_Sum_LCLCFX"), col("in1.`gl account number`").alias("gl account number"))
