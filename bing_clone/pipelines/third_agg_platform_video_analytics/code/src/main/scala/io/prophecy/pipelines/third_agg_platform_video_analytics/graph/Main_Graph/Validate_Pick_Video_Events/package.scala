package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Validate_Pick_Video_Events {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Reformat_1_2        = Reformat_1_2(context,        in)
    val df_Sort_1_2            = Sort_1_2(context,            df_Reformat_1_2)
    val df_Rollup_1_2          = Rollup_1_2(context,          df_Sort_1_2)
    val df_Rollup_1_Reformat_1 = Rollup_1_Reformat_1(context, df_Rollup_1_2)
    val df_Rollup_1_1          = Rollup_1_1(context,          df_Rollup_1_Reformat_1)
    val df_Rollup_Reformat_1   = Rollup_Reformat_1(context,   df_Rollup_1_1)
    val df_Reformat_7          = Reformat_7(context,          df_Rollup_Reformat_1)
    val df_Reformat_1_1        = Reformat_1_1(context,        df_Reformat_7)
    val df_Reformat_flatten    = Reformat_flatten(context,    df_Reformat_1_1)
    val df_Reformat_9          = Reformat_9(context,          df_Reformat_flatten)
    df_Reformat_9
  }

}
