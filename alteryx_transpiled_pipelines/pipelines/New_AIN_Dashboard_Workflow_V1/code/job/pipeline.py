from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_NewAINDashboard_6 = NewAINDashboard_6(spark)
    df_Filter_34 = Filter_34(spark, df_NewAINDashboard_6)
    df_AlteryxSelect_9 = AlteryxSelect_9(spark, df_Filter_34)
    df_AlteryxSelect_23 = AlteryxSelect_23(spark, df_Filter_34)
    df_Transpose_24 = Transpose_24(spark, df_AlteryxSelect_23)
    df_AlteryxSelect_25 = AlteryxSelect_25(spark, df_Transpose_24)
    df_AlteryxSelect_7 = AlteryxSelect_7(spark, df_Filter_34)
    df_Transpose_10 = Transpose_10(spark, df_AlteryxSelect_7)
    df_AlteryxSelect_11 = AlteryxSelect_11(spark, df_Transpose_10)
    df_Formula_12 = Formula_12(spark, df_AlteryxSelect_11)
    df_Union_15_reformat_0 = Union_15_reformat_0(spark, df_Formula_12)
    df_Transpose_13 = Transpose_13(spark, df_AlteryxSelect_9)
    df_AlteryxSelect_14 = AlteryxSelect_14(spark, df_Transpose_13)
    df_Union_15_reformat_1 = Union_15_reformat_1(spark, df_AlteryxSelect_14)
    df_Union_15 = Union_15(spark, df_Union_15_reformat_0, df_Union_15_reformat_1)
    df_AlteryxSelect_16 = AlteryxSelect_16(spark, df_Filter_34)
    df_Transpose_17 = Transpose_17(spark, df_AlteryxSelect_16)
    df_NewAINDashboard_1 = NewAINDashboard_1(spark)
    df_AlteryxSelect_19 = AlteryxSelect_19(spark, df_Filter_34)
    df_Transpose_20 = Transpose_20(spark, df_AlteryxSelect_19)
    df_AlteryxSelect_21 = AlteryxSelect_21(spark, df_Transpose_20)
    NewAINDashboard_28(spark, df_AlteryxSelect_21)
    df_AlteryxSelect_18 = AlteryxSelect_18(spark, df_Transpose_17)
    NewAINDashboard_27(spark, df_AlteryxSelect_18)
    df_Filter_35 = Filter_35(spark, df_NewAINDashboard_1)
    df_AlteryxSelect_2 = AlteryxSelect_2(spark, df_Filter_35)
    df_Transpose_3 = Transpose_3(spark, df_AlteryxSelect_2)
    df_AlteryxSelect_4 = AlteryxSelect_4(spark, df_Transpose_3)
    NewAINDashboard_29(spark, df_AlteryxSelect_25)
    NewAINDashboard_5(spark, df_AlteryxSelect_4)
    NewAINDashboard_26(spark, df_Union_15)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/New_AIN_Dashboard_Workflow_V1")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/New_AIN_Dashboard_Workflow_V1", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
