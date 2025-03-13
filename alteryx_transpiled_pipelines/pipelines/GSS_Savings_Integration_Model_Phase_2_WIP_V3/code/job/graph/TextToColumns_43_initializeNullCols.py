from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_43_initializeNullCols(spark: SparkSession, in0: DataFrame) -> DataFrame:
    
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    columns = [f"Name{i}" for i in range(1, 3)]
    updatedDF = in0

    for colName in columns:
        if colName not in updatedDF.columns:
            updatedDF = updatedDF.withColumn(colName, F.lit(None).cast(StringType()))

    out0 = updatedDF

    return out0
