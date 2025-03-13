from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_183_left_UnionLeftOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`cost center number`") == col("in1.`CC Lookup`")), "leftouter")\
        .select(col("in0.`pa account desc`").alias("pa account desc"), col("in0.`scenario name`").alias("scenario name"), col("in0.`company code`").alias("company code"), col("in0.`sub mc`").alias("sub mc"), col("in0.site").alias("site"), col("in0.year").alias("year"), col("in0.`material group`").alias("material group"), col("in1.EPM_ESS_Function_ASHB_Attr").alias("ASHB"), col("in0.quarter").alias("quarter"), col("in0.`hyperion category code`").alias("hyperion category code"), col("in0.company").alias("company"), col("in0.version").alias("version"), col("in0.`scenario type`").alias("scenario type"), col("in0.`planning sku`").alias("planning sku"), col("in0.Sum_Adjusted_Sum_USDAFX").alias("Sum_Adjusted_Sum_USDAFX"), col("in0.`cost center number`").alias("cost center number"), col("in0.`gl account`").alias("gl account"), col("in0.`hyperion category`").alias("hyperion category"), col("in0.evp").alias("evp"), col("in0.`CTS v3`").alias("CTS v3"), col("in0.`pa account`").alias("pa account"), col("in0.Sum_Adjusted_Sum_LCL").alias("Sum_Adjusted_Sum_LCL"), col("in0.Category").alias("Category"), col("in0.`material group description`").alias("material group description"), col("in0.mc").alias("mc"), col("in0.hlmc").alias("hlmc"), col("in0.`regrouped level 4`").alias("regrouped level 4"), col("in0.`cost center`").alias("cost center"), col("in0.`product description`").alias("product description"), col("in0.Sum_Adjusted_Sum_LCLCFX").alias("Sum_Adjusted_Sum_LCLCFX"), col("in0.`gl account number`").alias("gl account number"))
