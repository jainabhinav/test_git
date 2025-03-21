from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_181(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    nonEmptyDf = [x for x in [in0, in1, in2] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionByName(inDF, allowMissingColumns = True)

    return res
