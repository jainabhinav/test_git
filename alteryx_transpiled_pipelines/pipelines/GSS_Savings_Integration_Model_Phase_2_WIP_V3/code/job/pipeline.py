from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Savings_Distrib_38 = Savings_Distrib_38(spark)
    df_Transpose_40_schemaTransform = Transpose_40_schemaTransform(spark, df_Savings_Distrib_38)
    df_Transpose_40 = Transpose_40(spark, df_Transpose_40_schemaTransform)
    df_TextToColumns_42_sequence = TextToColumns_42_sequence(spark, df_Transpose_40)
    df_TextToColumns_42 = TextToColumns_42(spark, df_TextToColumns_42_sequence)
    df_TextToColumns_42_getPivotCol = TextToColumns_42_getPivotCol(spark, df_TextToColumns_42)
    df_TextToColumns_42_renamePivot = TextToColumns_42_renamePivot(spark, df_TextToColumns_42_getPivotCol)
    df_TextToColumns_42_explodeHorizontal = TextToColumns_42_explodeHorizontal(spark, df_TextToColumns_42_renamePivot)
    df_TextToColumns_42_initializeNullCols = TextToColumns_42_initializeNullCols(
        spark, 
        df_TextToColumns_42_explodeHorizontal
    )
    df_TextToColumns_42_cleanup = TextToColumns_42_cleanup(spark, df_TextToColumns_42_initializeNullCols)
    df_AlteryxSelect_44 = AlteryxSelect_44(spark, df_TextToColumns_42_cleanup)
    df_Savings_Distrib_39 = Savings_Distrib_39(spark)
    df_Transpose_41 = Transpose_41(spark, df_Savings_Distrib_39)
    df_TextToColumns_43_sequence = TextToColumns_43_sequence(spark, df_Transpose_41)
    df_TextToColumns_43 = TextToColumns_43(spark, df_TextToColumns_43_sequence)
    df_TextToColumns_43_getPivotCol = TextToColumns_43_getPivotCol(spark, df_TextToColumns_43)
    df_TextToColumns_43_renamePivot = TextToColumns_43_renamePivot(spark, df_TextToColumns_43_getPivotCol)
    df_TextToColumns_43_explodeHorizontal = TextToColumns_43_explodeHorizontal(spark, df_TextToColumns_43_renamePivot)
    df_TextToColumns_43_initializeNullCols = TextToColumns_43_initializeNullCols(
        spark, 
        df_TextToColumns_43_explodeHorizontal
    )
    df_TextToColumns_43_cleanup = TextToColumns_43_cleanup(spark, df_TextToColumns_43_initializeNullCols)
    df_AlteryxSelect_45 = AlteryxSelect_45(spark, df_TextToColumns_43_cleanup)
    df_Join_46_left_UnionFullOuter = Join_46_left_UnionFullOuter(spark, df_AlteryxSelect_44, df_AlteryxSelect_45)
    df_AlteryxSelect_48 = AlteryxSelect_48(spark, df_Join_46_left_UnionFullOuter)
    df_SavingsbyMegaCa_1 = SavingsbyMegaCa_1(spark)
    df_AlteryxSelect_20 = AlteryxSelect_20(spark, df_SavingsbyMegaCa_1)
    df_Join_3_inner = Join_3_inner(spark, df_AlteryxSelect_20, df_AlteryxSelect_48)
    df_Formula_4 = Formula_4(spark, df_Join_3_inner)
    df_AlteryxSelect_9 = AlteryxSelect_9(spark, df_Formula_4)
    df_Amgen_Summarize_10 = Amgen_Summarize_10(spark)
    df_AlteryxSelect_13 = AlteryxSelect_13(spark, df_Amgen_Summarize_10)
    df_Formula_12 = Formula_12(spark, df_AlteryxSelect_13)
    df_Summarize_62 = Summarize_62(spark, df_Formula_12)
    df_Filter_54 = Filter_54(spark, df_Summarize_62)
    df_Join_63_left = Join_63_left(spark, df_Formula_12, df_Filter_54)
    df_Join_63_inner = Join_63_inner(spark, df_Formula_12, df_Filter_54)
    df_Summarize_11 = Summarize_11(spark, df_Join_63_inner)
    df_Summarize_14 = Summarize_14(spark, df_Summarize_11)
    df_Join_15_inner = Join_15_inner(spark, df_Summarize_11, df_Summarize_14)
    df_Formula_16 = Formula_16(spark, df_Join_15_inner)
    df_Join_18_inner = Join_18_inner(spark, df_Join_63_inner, df_Formula_16)
    df_Join_19_inner = Join_19_inner(spark, df_Join_18_inner, df_AlteryxSelect_9)
    df_Formula_22 = Formula_22(spark, df_Join_19_inner)
    df_Union_31_reformat_0 = Union_31_reformat_0(spark, df_Formula_22)
    df_AlteryxSelect_55 = AlteryxSelect_55(spark, df_Join_63_left)
    df_Union_31_reformat_1 = Union_31_reformat_1(spark, df_AlteryxSelect_55)
    df_Join_19_left = Join_19_left(spark, df_Join_18_inner, df_AlteryxSelect_9)
    df_Union_31_reformat_2 = Union_31_reformat_2(spark, df_Join_19_left)
    df_Union_31 = Union_31(spark, df_Union_31_reformat_2, df_Union_31_reformat_0, df_Union_31_reformat_1)
    df_AlteryxSelect_24 = AlteryxSelect_24(spark, df_Union_31)
    Amgen_Summarize_37(spark, df_AlteryxSelect_24)
    df_Summarize_6 = Summarize_6(spark, df_AlteryxSelect_9)
    df_Join_19_right = Join_19_right(spark, df_AlteryxSelect_9, df_Join_18_inner)
    df_Summarize_30 = Summarize_30(spark, df_Join_19_right)
    df_Summarize_49 = Summarize_49(spark, df_Join_3_inner)
    Amgen_GSSSaving_50(spark, df_Summarize_49)
    df_Summarize_23 = Summarize_23(spark, df_Formula_22)
    Amgen_GSSSaving_32(spark, df_Summarize_30)
    Amgen_GSSSaving_29(spark, df_Summarize_23)
    Amgen_GSSSaving_33(spark, df_Summarize_6)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/GSS_Savings_Integration_Model_Phase_2_WIP_V3")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/GSS_Savings_Integration_Model_Phase_2_WIP_V3",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
