from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_197_explodeHorizontal(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("_seq"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`CTS Category`").alias("CTS Category")
    )
    df2 = df1.pivot("_pivot", ["1", "2"])

    return df2.agg(max(col("`_temp_Planning Account`")).alias("_temp_Planning Account"))
