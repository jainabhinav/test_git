from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from test_pipeline.config.ConfigStore import *
from test_pipeline.functions import *
from prophecy.utils import *
from test_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    s3_file_operations(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("test_pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test_pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/test_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
