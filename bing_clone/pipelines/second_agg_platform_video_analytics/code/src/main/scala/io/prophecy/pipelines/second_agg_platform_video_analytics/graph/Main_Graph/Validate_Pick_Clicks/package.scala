package io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Clicks.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Validate_Pick_Clicks {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Reformat_1_3        = Reformat_1_3(context,        in)
    val df_Sort_1_3            = Sort_1_3(context,            df_Reformat_1_3)
    val df_Rollup_1_3          = Rollup_1_3(context,          df_Sort_1_3)
    val df_Rollup_1_Reformat_2 = Rollup_1_Reformat_2(context, df_Rollup_1_3)
    val df_Rollup_2            = Rollup_2(context,            df_Rollup_1_Reformat_2)
    val df_Rollup_Reformat_2   = Rollup_Reformat_2(context,   df_Rollup_2)
    val df_Reformat_2          = Reformat_2(context,          df_Rollup_Reformat_2)
    val df_Reformat_8          = Reformat_8(context,          df_Reformat_2)
    df_Reformat_8
  }

}
