package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Validate_Pick_impbus_Clicks_imp_type_6 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Reformat_1_Filter_select = Reformat_1_Filter_select(context, in)
    val df_Reformat_1_Reformat =
      Reformat_1_Reformat(context, df_Reformat_1_Filter_select)
    val df_Sort_1_4            = Sort_1_4(context,            df_Reformat_1_Reformat)
    val df_Rollup_1_4          = Rollup_1_4(context,          df_Sort_1_4)
    val df_Rollup_1_Reformat_3 = Rollup_1_Reformat_3(context, df_Rollup_1_4)
    val df_Rollup_3            = Rollup_3(context,            df_Rollup_1_Reformat_3)
    val df_Rollup_Reformat_3   = Rollup_Reformat_3(context,   df_Rollup_3)
    val df_Reformat_3          = Reformat_3(context,          df_Rollup_Reformat_3)
    val df_Reformat_10         = Reformat_10(context,         df_Reformat_3)
    df_Reformat_10
  }

}
