from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Cleanse_70(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "tableau display category" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'tableau display category' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["tableau display category"].dataType, StringType):
            transformed_columns = [trim(col("tableau display category")).alias("tableau display category")]
        elif isinstance(
            in0.na.drop(how = "all").schema["tableau display category"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns = [col("tableau display category")]
        else:
            transformed_columns = [col("tableau display category")]

    return in0.na\
        .drop(how = "all")\
        .select(*[col(c) for c in in0.na.drop(how = "all").columns if c not in ["tableau display category"]], *transformed_columns)
