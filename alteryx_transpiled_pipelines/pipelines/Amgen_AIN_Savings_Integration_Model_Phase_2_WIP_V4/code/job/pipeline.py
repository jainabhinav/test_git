from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_CashSpendASHBLo_9 = CashSpendASHBLo_9(spark)
    df_AlteryxSelect_33 = AlteryxSelect_33(spark, df_CashSpendASHBLo_9)
    df_Unique_32 = Unique_32(spark, df_AlteryxSelect_33)
    df_CashSpendOSELab_7 = CashSpendOSELab_7(spark)
    df_Transpose_26 = Transpose_26(spark, df_CashSpendOSELab_7)
    df_AlteryxSelect_27 = AlteryxSelect_27(spark, df_Transpose_26)
    df_Unique_31 = Unique_31(spark, df_AlteryxSelect_27)
    df_CashSpendCTSV3L_12 = CashSpendCTSV3L_12(spark)
    df_AlteryxSelect_20 = AlteryxSelect_20(spark, df_CashSpendCTSV3L_12)
    df_Unique_19 = Unique_19(spark, df_AlteryxSelect_20)
    df_Amgen_CashSpend_2 = Amgen_CashSpend_2(spark)
    df_AlteryxSelect_15 = AlteryxSelect_15(spark, df_Amgen_CashSpend_2)
    df_Filter_43 = Filter_43(spark, df_AlteryxSelect_15)
    df_AlteryxSelect_21 = AlteryxSelect_21(spark, df_CashSpendCTSV3L_12)
    df_Unique_18 = Unique_18(spark, df_AlteryxSelect_21)
    df_Join_16_left_UnionLeftOuter = Join_16_left_UnionLeftOuter(spark, df_Filter_43, df_Unique_18)
    df_Join_17_left_UnionLeftOuter = Join_17_left_UnionLeftOuter(spark, df_Join_16_left_UnionLeftOuter, df_Unique_19)
    df_Formula_24 = Formula_24(spark, df_Join_17_left_UnionLeftOuter)
    df_AlteryxSelect_23 = AlteryxSelect_23(spark, df_Formula_24)
    df_Join_25_left = Join_25_left(spark, df_AlteryxSelect_23, df_Unique_31)
    df_Formula_28 = Formula_28(spark, df_Join_25_left)
    df_Union_29_variable1 = Union_29_variable1(spark, df_Formula_28)
    df_Union_29_reformat_0 = Union_29_reformat_0(spark, df_Union_29_variable1)
    df_Join_25_inner = Join_25_inner(spark, df_AlteryxSelect_23, df_Unique_31)
    df_Union_29_variable2 = Union_29_variable2(spark, df_Join_25_inner)
    df_Union_29_reformat_1 = Union_29_reformat_1(spark, df_Union_29_variable2)
    df_Union_29 = Union_29(spark, df_Union_29_reformat_1, df_Union_29_reformat_0)
    df_Union_29_cleanup = Union_29_cleanup(spark, df_Union_29)
    df_Join_30_inner = Join_30_inner(spark, df_Union_29_cleanup, df_Unique_32)
    df_Filter_35 = Filter_35(spark, df_Join_30_inner)
    df_Filter_46 = Filter_46(spark, df_Filter_35)
    df_In_ScopeCTS_xls_6 = In_ScopeCTS_xls_6(spark)
    df_Join_36_inner = Join_36_inner(spark, df_Filter_46, df_In_ScopeCTS_xls_6)
    df_AlteryxSelect_34 = AlteryxSelect_34(spark, df_Join_36_inner)
    df_Summarize_37 = Summarize_37(spark, df_AlteryxSelect_34)
    df_Filter_38 = Filter_38(spark, df_Summarize_37)
    df_Formula_52 = Formula_52(spark, df_Filter_38)
    df_Summarize_63 = Summarize_63(spark, df_Formula_52)
    df_AINSavingsInput_8 = AINSavingsInput_8(spark)
    df_Transpose_56 = Transpose_56(spark, df_AINSavingsInput_8)
    df_TextToColumns_57_sequence = TextToColumns_57_sequence(spark, df_Transpose_56)
    df_TextToColumns_57 = TextToColumns_57(spark, df_TextToColumns_57_sequence)
    df_TextToColumns_57_getPivotCol = TextToColumns_57_getPivotCol(spark, df_TextToColumns_57)
    df_TextToColumns_57_renamePivot = TextToColumns_57_renamePivot(spark, df_TextToColumns_57_getPivotCol)
    df_TextToColumns_57_explodeHorizontal = TextToColumns_57_explodeHorizontal(spark, df_TextToColumns_57_renamePivot)
    df_TextToColumns_57_initializeNullCols = TextToColumns_57_initializeNullCols(
        spark, 
        df_TextToColumns_57_explodeHorizontal
    )
    df_TextToColumns_57_cleanup = TextToColumns_57_cleanup(spark, df_TextToColumns_57_initializeNullCols)
    df_AlteryxSelect_58 = AlteryxSelect_58(spark, df_TextToColumns_57_cleanup)
    df_CrossTab_59 = CrossTab_59(spark, df_AlteryxSelect_58)
    df_CrossTab_59_regularActions = CrossTab_59_regularActions(spark, df_CrossTab_59)
    df_CrossTab_59_rename = CrossTab_59_rename(spark, df_CrossTab_59_regularActions)
    df_Formula_60 = Formula_60(spark, df_CrossTab_59_rename)
    df_Formula_65 = Formula_65(spark, df_Formula_60)
    df_AppendFields_83 = AppendFields_83(spark, df_Summarize_63, df_Formula_65)
    df_Formula_349 = Formula_349(spark, df_AppendFields_83)
    df_Filter_355 = Filter_355(spark, df_Formula_349)
    df_Filter_354 = Filter_354(spark, df_Filter_355)
    df_Summarize_360 = Summarize_360(spark, df_Filter_354)
    df_Summarize_51 = Summarize_51(spark, df_Formula_52)
    df_Join_53_inner = Join_53_inner(spark, df_Formula_52, df_Summarize_51)
    df_Formula_54 = Formula_54(spark, df_Join_53_inner)
    df_AlteryxSelect_341 = AlteryxSelect_341(spark, df_Formula_54)
    df_Summarize_145 = Summarize_145(spark, df_Formula_54)
    df_Summarize_137 = Summarize_137(spark, df_AppendFields_83)
    df_Filter_138 = Filter_138(spark, df_Summarize_137)
    df_Formula_139 = Formula_139(spark, df_Filter_138)
    df_Join_140_left_UnionLeftOuter = Join_140_left_UnionLeftOuter(spark, df_AppendFields_83, df_Formula_139)
    df_Join_142_inner = Join_142_inner(spark, df_Summarize_145, df_Join_140_left_UnionLeftOuter)
    df_Join_143_right = Join_143_right(spark, df_Join_142_inner, df_Formula_54)
    df_Formula_171 = Formula_171(spark, df_Join_143_right)
    df_Union_146_reformat_0 = Union_146_reformat_0(spark, df_Formula_171)
    df_Join_142_right = Join_142_right(spark, df_Join_140_left_UnionLeftOuter, df_Summarize_145)
    df_Filter_173 = Filter_173(spark, df_Join_142_right)
    df_Formula_174 = Formula_174(spark, df_Filter_173)
    df_Union_146_reformat_1 = Union_146_reformat_1(spark, df_Formula_174)
    df_Join_143_inner = Join_143_inner(spark, df_Formula_54, df_Join_142_inner)
    df_AlteryxSelect_147 = AlteryxSelect_147(spark, df_Join_143_inner)
    df_Union_146_reformat_2 = Union_146_reformat_2(spark, df_AlteryxSelect_147)
    df_Union_146 = Union_146(spark, df_Union_146_reformat_0, df_Union_146_reformat_1, df_Union_146_reformat_2)
    df_Union_152_reformat_0 = Union_152_reformat_0(spark, df_Union_146)
    df_Formula_148 = Formula_148(spark, df_Union_146)
    df_Summarize_149 = Summarize_149(spark, df_Formula_148)
    df_Filter_173_reject = Filter_173_reject(spark, df_Join_142_right)
    df_Join_150_inner = Join_150_inner(spark, df_Summarize_149, df_Filter_173_reject)
    df_Summarize_202 = Summarize_202(spark, df_Join_150_inner)
    df_Filter_203 = Filter_203(spark, df_Summarize_202)
    df_Join_204_left = Join_204_left(spark, df_Join_150_inner, df_Filter_203)
    df_AlteryxSelect_151 = AlteryxSelect_151(spark, df_Join_204_left)
    df_Union_152_reformat_1 = Union_152_reformat_1(spark, df_AlteryxSelect_151)
    df_Union_152 = Union_152(spark, df_Union_152_reformat_0, df_Union_152_reformat_1)
    df_Union_156_reformat_0 = Union_156_reformat_0(spark, df_Union_152)
    df_Summarize_153 = Summarize_153(spark, df_Union_152)
    df_Join_150_right = Join_150_right(spark, df_Filter_173_reject, df_Summarize_149)
    df_Union_205_reformat_0 = Union_205_reformat_0(spark, df_Join_150_right)
    df_Join_204_inner = Join_204_inner(spark, df_Join_150_inner, df_Filter_203)
    df_Union_205_reformat_1 = Union_205_reformat_1(spark, df_Join_204_inner)
    df_Union_205 = Union_205(spark, df_Union_205_reformat_0, df_Union_205_reformat_1)
    df_Join_154_inner = Join_154_inner(spark, df_Summarize_153, df_Union_205)
    df_Summarize_206 = Summarize_206(spark, df_Join_154_inner)
    df_Filter_207 = Filter_207(spark, df_Summarize_206)
    df_Join_208_left = Join_208_left(spark, df_Join_154_inner, df_Filter_207)
    df_AlteryxSelect_155 = AlteryxSelect_155(spark, df_Join_208_left)
    df_Union_156_reformat_1 = Union_156_reformat_1(spark, df_AlteryxSelect_155)
    df_Union_156 = Union_156(spark, df_Union_156_reformat_0, df_Union_156_reformat_1)
    df_Union_160_reformat_0 = Union_160_reformat_0(spark, df_Union_156)
    df_Summarize_157 = Summarize_157(spark, df_Union_156)
    df_Join_154_right = Join_154_right(spark, df_Union_205, df_Summarize_153)
    df_Union_209_reformat_0 = Union_209_reformat_0(spark, df_Join_154_right)
    df_Join_208_inner = Join_208_inner(spark, df_Join_154_inner, df_Filter_207)
    df_Union_209_reformat_1 = Union_209_reformat_1(spark, df_Join_208_inner)
    df_Union_209 = Union_209(spark, df_Union_209_reformat_0, df_Union_209_reformat_1)
    df_Join_158_inner = Join_158_inner(spark, df_Summarize_157, df_Union_209)
    df_Summarize_210 = Summarize_210(spark, df_Join_158_inner)
    df_Filter_211 = Filter_211(spark, df_Summarize_210)
    df_Join_212_left = Join_212_left(spark, df_Join_158_inner, df_Filter_211)
    df_AlteryxSelect_159 = AlteryxSelect_159(spark, df_Join_212_left)
    df_Union_160_reformat_1 = Union_160_reformat_1(spark, df_AlteryxSelect_159)
    df_Union_160 = Union_160(spark, df_Union_160_reformat_0, df_Union_160_reformat_1)
    df_Union_164_reformat_0 = Union_164_reformat_0(spark, df_Union_160)
    df_Summarize_162 = Summarize_162(spark, df_Union_160)
    df_Join_158_right = Join_158_right(spark, df_Union_209, df_Summarize_157)
    df_Union_213_reformat_0 = Union_213_reformat_0(spark, df_Join_158_right)
    df_Join_212_inner = Join_212_inner(spark, df_Join_158_inner, df_Filter_211)
    df_Union_213_reformat_1 = Union_213_reformat_1(spark, df_Join_212_inner)
    df_Union_213 = Union_213(spark, df_Union_213_reformat_0, df_Union_213_reformat_1)
    df_Join_161_inner = Join_161_inner(spark, df_Summarize_162, df_Union_213)
    df_AlteryxSelect_165 = AlteryxSelect_165(spark, df_Join_161_inner)
    df_Union_164_reformat_1 = Union_164_reformat_1(spark, df_AlteryxSelect_165)
    df_Union_164 = Union_164(spark, df_Union_164_reformat_0, df_Union_164_reformat_1)
    df_Summarize_166 = Summarize_166(spark, df_Union_164)
    df_Formula_214 = Formula_214(spark, df_Summarize_166)
    df_AlteryxSelect_216 = AlteryxSelect_216(spark, df_Union_164)
    df_Join_215_inner = Join_215_inner(spark, df_AlteryxSelect_216, df_Formula_214)
    df_AlteryxSelect_217 = AlteryxSelect_217(spark, df_Join_215_inner)
    df_Formula_218 = Formula_218(spark, df_AlteryxSelect_217)
    df_Filter_359 = Filter_359(spark, df_Formula_218)
    df_Filter_400 = Filter_400(spark, df_Filter_359)
    df_AlteryxSelect_342 = AlteryxSelect_342(spark, df_Filter_400)
    df_Join_340_right = Join_340_right(spark, df_AlteryxSelect_342, df_AlteryxSelect_341)
    df_Join_344_left = Join_344_left(spark, df_Join_340_right, df_Summarize_360)
    df_Join_344_right = Join_344_right(spark, df_Summarize_360, df_Join_340_right)
    df_Join_346_left = Join_346_left(spark, df_Join_344_left, df_Join_344_right)
    df_Join_346_right = Join_346_right(spark, df_Join_344_right, df_Join_344_left)
    df_Join_348_left = Join_348_left(spark, df_Join_346_left, df_Join_346_right)
    df_Join_348_right = Join_348_right(spark, df_Join_346_right, df_Join_346_left)
    df_Join_351_left = Join_351_left(spark, df_Join_348_left, df_Join_348_right)
    df_Union_353_reformat_5 = Union_353_reformat_5(spark, df_Join_351_left)
    df_AINSavingsInput_226 = AINSavingsInput_226(spark)
    df_AlteryxSelect_231 = AlteryxSelect_231(spark, df_AINSavingsInput_226)
    df_Transpose_227 = Transpose_227(spark, df_AlteryxSelect_231)
    df_TextToColumns_228_sequence = TextToColumns_228_sequence(spark, df_Transpose_227)
    df_TextToColumns_228 = TextToColumns_228(spark, df_TextToColumns_228_sequence)
    df_TextToColumns_228_getPivotCol = TextToColumns_228_getPivotCol(spark, df_TextToColumns_228)
    df_TextToColumns_228_renamePivot = TextToColumns_228_renamePivot(spark, df_TextToColumns_228_getPivotCol)
    AmgenAINOSELabo_426(spark, df_AlteryxSelect_27)
    df_AlteryxSelect_221 = AlteryxSelect_221(spark, df_Formula_218)
    df_Filter_261 = Filter_261(spark, df_AlteryxSelect_221)
    df_Formula_299 = Formula_299(spark, df_Filter_261)
    df_In_ScopeCTS_xls_190 = In_ScopeCTS_xls_190(spark)
    df_HyperionOSELabo_185 = HyperionOSELabo_185(spark)
    df_TextToColumns_194_sequence = TextToColumns_194_sequence(spark, df_HyperionOSELabo_185)
    df_TextToColumns_194 = TextToColumns_194(spark, df_TextToColumns_194_sequence)
    df_TextToColumns_194_getPivotCol = TextToColumns_194_getPivotCol(spark, df_TextToColumns_194)
    df_TextToColumns_194_explodeHorizontal = TextToColumns_194_explodeHorizontal(
        spark, 
        df_TextToColumns_194_getPivotCol
    )
    df_TextToColumns_194_initializeNullCols = TextToColumns_194_initializeNullCols(
        spark, 
        df_TextToColumns_194_explodeHorizontal
    )
    df_TextToColumns_194_cleanup = TextToColumns_194_cleanup(spark, df_TextToColumns_194_initializeNullCols)
    df_AlteryxSelect_195 = AlteryxSelect_195(spark, df_TextToColumns_194_cleanup)
    df_Formula_196 = Formula_196(spark, df_AlteryxSelect_195)
    df_Amgen_HyperionO_175 = Amgen_HyperionO_175(spark)
    df_Filter_256 = Filter_256(spark, df_Amgen_HyperionO_175)
    df_AlteryxSelect_255 = AlteryxSelect_255(spark, df_Filter_256)
    df_HyperionCTSLook_176 = HyperionCTSLook_176(spark)
    df_TextToColumns_197_sequence = TextToColumns_197_sequence(spark, df_HyperionCTSLook_176)
    df_TextToColumns_197 = TextToColumns_197(spark, df_TextToColumns_197_sequence)
    df_TextToColumns_197_getPivotCol = TextToColumns_197_getPivotCol(spark, df_TextToColumns_197)
    df_TextToColumns_197_explodeHorizontal = TextToColumns_197_explodeHorizontal(
        spark, 
        df_TextToColumns_197_getPivotCol
    )
    df_TextToColumns_197_initializeNullCols = TextToColumns_197_initializeNullCols(
        spark, 
        df_TextToColumns_197_explodeHorizontal
    )
    df_TextToColumns_197_cleanup = TextToColumns_197_cleanup(spark, df_TextToColumns_197_initializeNullCols)
    df_AlteryxSelect_198 = AlteryxSelect_198(spark, df_TextToColumns_197_cleanup)
    df_Formula_199 = Formula_199(spark, df_AlteryxSelect_198)
    df_Join_177_inner = Join_177_inner(spark, df_AlteryxSelect_255, df_Formula_199)
    df_Union_181_reformat_0 = Union_181_reformat_0(spark, df_Join_177_inner)
    df_HyperionCTSLook_178 = HyperionCTSLook_178(spark)
    df_AlteryxSelect_180 = AlteryxSelect_180(spark, df_HyperionCTSLook_178)
    df_Unique_259 = Unique_259(spark, df_AlteryxSelect_180)
    df_Join_177_left = Join_177_left(spark, df_AlteryxSelect_255, df_Formula_199)
    df_Join_179_left = Join_179_left(spark, df_Join_177_left, df_Unique_259)
    df_Union_181_reformat_1 = Union_181_reformat_1(spark, df_Join_179_left)
    df_Join_179_inner = Join_179_inner(spark, df_Join_177_left, df_Unique_259)
    df_Union_181_reformat_2 = Union_181_reformat_2(spark, df_Join_179_inner)
    df_Union_181 = Union_181(spark, df_Union_181_reformat_0, df_Union_181_reformat_2, df_Union_181_reformat_1)
    df_HyperionASHBLoo_182 = HyperionASHBLoo_182(spark)
    df_AlteryxSelect_184 = AlteryxSelect_184(spark, df_HyperionASHBLoo_182)
    df_Join_183_left_UnionLeftOuter = Join_183_left_UnionLeftOuter(spark, df_Union_181, df_AlteryxSelect_184)
    df_Join_187_left_UnionLeftOuter = Join_187_left_UnionLeftOuter(
        spark, 
        df_Join_183_left_UnionLeftOuter, 
        df_Formula_196
    )
    df_Join_189_left_UnionLeftOuter = Join_189_left_UnionLeftOuter(
        spark, 
        df_Join_187_left_UnionLeftOuter, 
        df_In_ScopeCTS_xls_190
    )
    df_Formula_406 = Formula_406(spark, df_Join_189_left_UnionLeftOuter)
    df_Filter_325 = Filter_325(spark, df_Formula_406)
    df_Filter_308 = Filter_308(spark, df_Filter_325)
    df_Filter_191 = Filter_191(spark, df_Filter_308)
    df_Filter_310 = Filter_310(spark, df_Filter_191)
    df_Filter_192 = Filter_192(spark, df_Filter_310)
    df_AlteryxSelect_225 = AlteryxSelect_225(spark, df_Filter_192)
    df_Summarize_235 = Summarize_235(spark, df_AlteryxSelect_225)
    df_Summarize_224 = Summarize_224(spark, df_Summarize_235)
    df_Join_236_inner = Join_236_inner(spark, df_Summarize_235, df_Summarize_224)
    df_Formula_237 = Formula_237(spark, df_Join_236_inner)
    df_Join_238_left = Join_238_left(spark, df_Formula_299, df_Formula_237)
    df_Filter_319 = Filter_319(spark, df_Join_238_left)
    df_Filter_326 = Filter_326(spark, df_Filter_319)
    df_Summarize_317 = Summarize_317(spark, df_Filter_326)
    df_Summarize_323 = Summarize_323(spark, df_Filter_325)
    df_AlteryxSelect_320 = AlteryxSelect_320(spark, df_Summarize_323)
    df_Join_312_left_UnionLeftOuter = Join_312_left_UnionLeftOuter(spark, df_Summarize_317, df_AlteryxSelect_320)
    df_Join_340_inner = Join_340_inner(spark, df_AlteryxSelect_341, df_AlteryxSelect_342)
    df_Formula_343 = Formula_343(spark, df_Join_340_inner)
    df_Union_353_reformat_0 = Union_353_reformat_0(spark, df_Formula_343)
    df_Join_344_inner = Join_344_inner(spark, df_Join_340_right, df_Summarize_360)
    df_Formula_345 = Formula_345(spark, df_Join_344_inner)
    df_Union_353_reformat_1 = Union_353_reformat_1(spark, df_Formula_345)
    df_Join_346_inner = Join_346_inner(spark, df_Join_344_left, df_Join_344_right)
    df_Formula_347 = Formula_347(spark, df_Join_346_inner)
    df_Union_353_reformat_2 = Union_353_reformat_2(spark, df_Formula_347)
    df_Join_348_inner = Join_348_inner(spark, df_Join_346_left, df_Join_346_right)
    df_Formula_350 = Formula_350(spark, df_Join_348_inner)
    df_Union_353_reformat_3 = Union_353_reformat_3(spark, df_Formula_350)
    df_Join_351_inner = Join_351_inner(spark, df_Join_348_left, df_Join_348_right)
    df_Formula_352 = Formula_352(spark, df_Join_351_inner)
    df_Union_353_reformat_4 = Union_353_reformat_4(spark, df_Formula_352)
    df_Union_353 = Union_353(
        spark, 
        df_Union_353_reformat_3, 
        df_Union_353_reformat_1, 
        df_Union_353_reformat_2, 
        df_Union_353_reformat_5, 
        df_Union_353_reformat_0, 
        df_Union_353_reformat_4
    )
    df_Filter_385 = Filter_385(spark, df_Union_353)
    df_Join_238_right = Join_238_right(spark, df_Formula_237, df_Formula_299)
    df_AlteryxSelect_247 = AlteryxSelect_247(spark, df_Join_238_right)
    df_Formula_421 = Formula_421(spark, df_AlteryxSelect_247)
    df_Summarize_422 = Summarize_422(spark, df_Formula_421)
    df_Filter_423 = Filter_423(spark, df_Summarize_422)
    df_Join_424_inner = Join_424_inner(spark, df_Formula_421, df_Filter_423)
    df_Summarize_244 = Summarize_244(spark, df_Join_424_inner)
    df_Join_245_inner = Join_245_inner(spark, df_Join_424_inner, df_Summarize_244)
    df_Formula_246 = Formula_246(spark, df_Join_245_inner)
    df_TextToColumns_228_explodeHorizontal = TextToColumns_228_explodeHorizontal(
        spark, 
        df_TextToColumns_228_renamePivot
    )
    df_TextToColumns_228_initializeNullCols = TextToColumns_228_initializeNullCols(
        spark, 
        df_TextToColumns_228_explodeHorizontal
    )
    df_TextToColumns_228_cleanup = TextToColumns_228_cleanup(spark, df_TextToColumns_228_initializeNullCols)
    df_AlteryxSelect_229 = AlteryxSelect_229(spark, df_TextToColumns_228_cleanup)
    df_CrossTab_230 = CrossTab_230(spark, df_AlteryxSelect_229)
    df_CrossTab_230_regularActions = CrossTab_230_regularActions(spark, df_CrossTab_230)
    df_CrossTab_230_rename = CrossTab_230_rename(spark, df_CrossTab_230_regularActions)
    df_Formula_249 = Formula_249(spark, df_CrossTab_230_rename)
    df_Filter_250 = Filter_250(spark, df_Formula_249)
    df_Join_248_inner = Join_248_inner(spark, df_Filter_250, df_Formula_246)
    df_Formula_252 = Formula_252(spark, df_Join_248_inner)
    df_AlteryxSelect_268 = AlteryxSelect_268(spark, df_Formula_252)
    df_Summarize_425 = Summarize_425(spark, df_Formula_218)
    df_Join_238_inner = Join_238_inner(spark, df_Formula_299, df_Formula_237)
    df_Amgen_Summarize_265 = Amgen_Summarize_265(spark)
    df_Formula_407 = Formula_407(spark, df_Amgen_Summarize_265)
    df_Formula_239 = Formula_239(spark, df_Join_238_inner)
    df_AlteryxSelect_267 = AlteryxSelect_267(spark, df_Formula_239)
    df_Union_269_variable2 = Union_269_variable2(spark, df_AlteryxSelect_267)
    df_Union_269_reformat_0 = Union_269_reformat_0(spark, df_Union_269_variable2)
    df_Union_269_variable1 = Union_269_variable1(spark, df_AlteryxSelect_268)
    df_Union_269_reformat_1 = Union_269_reformat_1(spark, df_Union_269_variable1)
    df_Union_269 = Union_269(spark, df_Union_269_reformat_0, df_Union_269_reformat_1)
    df_Filter_334 = Filter_334(spark, df_Formula_65)
    df_Union_370_variable1 = Union_370_variable1(spark, df_Filter_334)
    df_Union_370_reformat_0 = Union_370_reformat_0(spark, df_Union_370_variable1)
    df_Union_370_variable2 = Union_370_variable2(spark, df_Filter_250)
    df_Union_370_reformat_1 = Union_370_reformat_1(spark, df_Union_370_variable2)
    df_Union_370 = Union_370(spark, df_Union_370_reformat_0, df_Union_370_reformat_1)
    df_Union_370_cleanup = Union_370_cleanup(spark, df_Union_370)
    df_Summarize_369 = Summarize_369(spark, df_Union_370_cleanup)
    df_Filter_401 = Filter_401(spark, df_Summarize_369)
    df_Formula_390 = Formula_390(spark, df_Filter_401)
    AmgenAINSavings_365(spark, df_Formula_390)
    df_Formula_364 = Formula_364(spark, df_Filter_385)
    df_AlteryxSelect_361 = AlteryxSelect_361(spark, df_Formula_364)
    df_Summarize_363 = Summarize_363(spark, df_AlteryxSelect_361)
    df_Union_269_cleanup = Union_269_cleanup(spark, df_Union_269)
    df_Summarize_286 = Summarize_286(spark, df_Union_269_cleanup)
    df_Formula_408 = Formula_408(spark, df_Summarize_286)
    df_Join_409_left_UnionLeftOuter = Join_409_left_UnionLeftOuter(spark, df_Formula_407, df_Formula_408)
    df_AlteryxSelect_276 = AlteryxSelect_276(spark, df_Join_409_left_UnionLeftOuter)
    AmgenAINSavingD_427(spark, df_Summarize_425)
    df_Summarize_302 = Summarize_302(spark, df_AlteryxSelect_276)
    df_Join_304_inner = Join_304_inner(spark, df_AlteryxSelect_276, df_Summarize_302)
    df_Formula_305 = Formula_305(spark, df_Join_304_inner)
    df_AlteryxSelect_379 = AlteryxSelect_379(spark, df_Formula_305)
    df_Union_388_variable1 = Union_388_variable1(spark, df_Summarize_363)
    df_Union_388_reformat_0 = Union_388_reformat_0(spark, df_Union_388_variable1)
    df_Formula_386 = Formula_386(spark, df_Formula_252)
    df_Summarize_387 = Summarize_387(spark, df_Formula_386)
    df_Union_388_variable2 = Union_388_variable2(spark, df_Summarize_387)
    df_Union_388_reformat_1 = Union_388_reformat_1(spark, df_Union_388_variable2)
    df_Union_388 = Union_388(spark, df_Union_388_reformat_0, df_Union_388_reformat_1)
    df_Union_388_cleanup = Union_388_cleanup(spark, df_Union_388)
    AmgenAINSavings_362(spark, df_Union_388_cleanup)
    df_Formula_322 = Formula_322(spark, df_Join_312_left_UnionLeftOuter)
    df_Summarize_327 = Summarize_327(spark, df_Formula_322)
    df_Join_328_inner = Join_328_inner(spark, df_Formula_322, df_Summarize_327)
    df_Formula_329 = Formula_329(spark, df_Join_328_inner)
    df_AlteryxSelect_306 = AlteryxSelect_306(spark, df_Formula_305)
    df_Filter_380 = Filter_380(spark, df_AlteryxSelect_379)
    df_AlteryxSelect_331 = AlteryxSelect_331(spark, df_Formula_329)
    AmgenAINSavings_368(spark, df_AlteryxSelect_331)
    df_Join_272_left_UnionLeftOuter = Join_272_left_UnionLeftOuter(spark, df_Formula_407, df_Formula_408)
    df_Summarize_371 = Summarize_371(spark, df_Formula_305)
    df_Summarize_373 = Summarize_373(spark, df_AlteryxSelect_276)
    df_Join_376_inner = Join_376_inner(spark, df_Summarize_373, df_Summarize_371)
    df_Formula_377 = Formula_377(spark, df_Join_376_inner)
    df_Filter_391 = Filter_391(spark, df_Formula_377)
    AmgenAINSavings_367(spark, df_Summarize_371)
    df_AlteryxSelect_378 = AlteryxSelect_378(spark, df_Filter_391)
    AmgenAINSavings_374(spark, df_AlteryxSelect_378)
    df_Filter_381 = Filter_381(spark, df_Filter_380)
    AmgenAINSavings_332(spark, df_AlteryxSelect_331)
    AmgenAINSavings_292(spark, df_AlteryxSelect_306)
    AmgenAINSavings_382(spark, df_Filter_381)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Amgen_AIN_Savings_Integration_Model_Phase_2_WIP_V4")
    registerUDFs(spark)
    
    MetricsCollector.instrument(
        spark = spark,
        pipelineId = "pipelines/Amgen_AIN_Savings_Integration_Model_Phase_2_WIP_V4",
        config = Config
    )(
        pipeline
    )

if __name__ == "__main__":
    main()
