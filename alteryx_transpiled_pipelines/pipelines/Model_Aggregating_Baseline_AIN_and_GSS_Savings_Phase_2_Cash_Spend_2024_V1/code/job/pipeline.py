from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Amgen_Summarize_4 = Amgen_Summarize_4(spark)
    df_AlteryxSelect_8 = AlteryxSelect_8(spark, df_Amgen_Summarize_4)
    df_MultiFieldFormula_45 = MultiFieldFormula_45(spark, df_AlteryxSelect_8)
    df_CashSpend2024da_98 = CashSpend2024da_98(spark)
    df_Tableaudisplaym_64 = Tableaudisplaym_64(spark)
    df_Cleanse_70 = Cleanse_70(spark, df_Tableaudisplaym_64)
    df_AlteryxSelect_65 = AlteryxSelect_65(spark, df_Cleanse_70)
    df_Summarize_67 = Summarize_67(spark, df_AlteryxSelect_65)
    df_TextInput_38 = TextInput_38(spark)
    df_TextInput_38_cast = TextInput_38_cast(spark, df_TextInput_38)
    df_Union_80_variable2 = Union_80_variable2(spark, df_TextInput_38_cast)
    df_Union_80_reformat_0 = Union_80_reformat_0(spark, df_Union_80_variable2)
    df_Summarize_66 = Summarize_66(spark, df_AlteryxSelect_65)
    df_Join_68_inner = Join_68_inner(spark, df_Summarize_67, df_Summarize_66)
    df_Formula_69 = Formula_69(spark, df_Join_68_inner)
    df_AlteryxSelect_71 = AlteryxSelect_71(spark, df_Formula_69)
    df_Amgen_Summarize_6 = Amgen_Summarize_6(spark)
    df_AlteryxSelect_20 = AlteryxSelect_20(spark, df_Amgen_Summarize_6)
    df_Join_10_inner = Join_10_inner(spark, df_AlteryxSelect_20, df_AlteryxSelect_8)
    df_AlteryxSelect_17 = AlteryxSelect_17(spark, df_Join_10_inner)
    df_AmgenAINSavings_5 = AmgenAINSavings_5(spark)
    df_AlteryxSelect_9 = AlteryxSelect_9(spark, df_AmgenAINSavings_5)
    df_Join_11_inner = Join_11_inner(spark, df_AlteryxSelect_17, df_AlteryxSelect_9)
    df_AlteryxSelect_12 = AlteryxSelect_12(spark, df_Join_11_inner)
    df_Formula_35 = Formula_35(spark, df_AlteryxSelect_12)
    df_Join_72_inner = Join_72_inner(spark, df_Formula_35, df_AlteryxSelect_71)
    df_AlteryxSelect_74 = AlteryxSelect_74(spark, df_Join_72_inner)
    df_MultiFieldFormula_75 = MultiFieldFormula_75(spark, df_AlteryxSelect_74)
    df_Union_80_variable1 = Union_80_variable1(spark, df_MultiFieldFormula_75)
    df_Union_80_reformat_1 = Union_80_reformat_1(spark, df_Union_80_variable1)
    df_Union_80 = Union_80(spark, df_Union_80_reformat_0, df_Union_80_reformat_1)
    df_Union_80_cleanup = Union_80_cleanup(spark, df_Union_80)
    df_AmgenNon_Addres_85 = AmgenNon_Addres_85(spark)
    df_AlteryxSelect_88 = AlteryxSelect_88(spark, df_AmgenNon_Addres_85)
    df_Formula_89 = Formula_89(spark, df_AlteryxSelect_88)
    df_Formula_59 = Formula_59(spark, df_Union_80_cleanup)
    df_AlteryxSelect_58 = AlteryxSelect_58(spark, df_Formula_59)
    df_Union_87_variable1 = Union_87_variable1(spark, df_AlteryxSelect_58)
    df_Union_87_reformat_0 = Union_87_reformat_0(spark, df_Union_87_variable1)
    df_Union_87_variable2 = Union_87_variable2(spark, df_Formula_89)
    df_Union_87_reformat_1 = Union_87_reformat_1(spark, df_Union_87_variable2)
    df_Union_87 = Union_87(spark, df_Union_87_reformat_0, df_Union_87_reformat_1)
    df_Union_87_cleanup = Union_87_cleanup(spark, df_Union_87)
    df_AlteryxSelect_95 = AlteryxSelect_95(spark, df_Union_87_cleanup)
    df_Union_99_variable1 = Union_99_variable1(spark, df_AlteryxSelect_95)
    df_Union_99_reformat_0 = Union_99_reformat_0(spark, df_Union_99_variable1)
    df_AlteryxSelect_100 = AlteryxSelect_100(spark, df_CashSpend2024da_98)
    df_Formula_109 = Formula_109(spark, df_AlteryxSelect_100)
    df_Formula_110 = Formula_110(spark, df_Formula_109)
    df_Union_99_variable2 = Union_99_variable2(spark, df_Formula_110)
    df_Union_99_reformat_1 = Union_99_reformat_1(spark, df_Union_99_variable2)
    df_Union_99 = Union_99(spark, df_Union_99_reformat_0, df_Union_99_reformat_1)
    df_Union_99_cleanup = Union_99_cleanup(spark, df_Union_99)
    df_Summarize_94 = Summarize_94(spark, df_Union_99_cleanup)
    df_Cleanse_84_before = Cleanse_84_before(spark, df_Summarize_94)
    df_Cleanse_84 = Cleanse_84(spark, df_Cleanse_84_before)
    df_Summarize_77 = Summarize_77(spark, df_MultiFieldFormula_75)
    AmgenSavingsSum_78(spark, df_Summarize_77)
    df_MultiFieldFormula_19 = MultiFieldFormula_19(spark, df_Formula_35)
    df_MultiFieldFormula_23 = MultiFieldFormula_23(spark, df_AlteryxSelect_9)
    df_Filter_15 = Filter_15(spark, df_Join_10_inner)
    df_Summarize_104 = Summarize_104(spark, df_Summarize_94)
    DisplayCategory_90(spark, df_AlteryxSelect_71)
    df_Summarize_44 = Summarize_44(spark, df_MultiFieldFormula_45)
    df_MultiFieldFormula_49 = MultiFieldFormula_49(spark, df_AlteryxSelect_9)
    df_Summarize_48 = Summarize_48(spark, df_MultiFieldFormula_49)
    AmgenSavingsSum_50(spark, df_Summarize_48)
    df_Filter_102 = Filter_102(spark, df_Summarize_94)
    Sample2023_xlsx_107(spark, df_Filter_102)
    AmgenSavingsSum_46(spark, df_Summarize_44)
    df_MultiFieldFormula_21 = MultiFieldFormula_21(spark, df_AlteryxSelect_8)
    df_Summarize_24 = Summarize_24(spark, df_MultiFieldFormula_21)
    df_Summarize_83 = Summarize_83(spark, df_Formula_35)
    df_Filter_16 = Filter_16(spark, df_Join_11_inner)
    AmgenSavingsSum_79(spark, df_Summarize_83)
    df_Filter_103 = Filter_103(spark, df_Summarize_94)
    df_Cleanse_84_after = Cleanse_84_after(spark, df_Cleanse_84)
    df_MultiFieldFormula_41 = MultiFieldFormula_41(spark, df_Formula_35)
    df_Summarize_42 = Summarize_42(spark, df_MultiFieldFormula_41)
    AmgenSavingsSum_43(spark, df_Summarize_42)
    df_Formula_97 = Formula_97(spark, df_Cleanse_84_after)
    AmgenModelAggre_57(spark, df_Formula_97)
    Sample2024_xlsx_106(spark, df_Filter_103)
    df_Filter_15_reject = Filter_15_reject(spark, df_Join_10_inner)
    df_Summarize_25 = Summarize_25(spark, df_MultiFieldFormula_23)
    df_Filter_16_reject = Filter_16_reject(spark, df_Join_11_inner)
    df_Summarize_91 = Summarize_91(spark, df_Summarize_94)
    AggregatedQCExt_92(spark, df_Summarize_91)
    df_Summarize_18 = Summarize_18(spark, df_MultiFieldFormula_19)
    AmgenSavingsSum_30(spark, df_Summarize_25)
    AmgenSavingsSum_29(spark, df_Summarize_24)
    AggregatedQCAIN_105(spark, df_Summarize_104)
    AmgenSavingsSum_28(spark, df_Summarize_18)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set(
        "prophecy.metadata.pipeline.uri",
        "pipelines/Model_Aggregating_Baseline_AIN_and_GSS_Savings_Phase_2_Cash_Spend_2024_V1"
    )
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/Model_Aggregating_Baseline_AIN_and_GSS_Savings_Phase_2_Cash_Spend_2024_V1",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
