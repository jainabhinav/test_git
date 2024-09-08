package io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Pixels.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Validate_Pick_Pixels {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Reformat_1        = Reformat_1(context,        in)
    val df_Sort_1            = Sort_1(context,            df_Reformat_1)
    val df_Rollup_1          = Rollup_1(context,          df_Sort_1)
    val df_Rollup_1_Reformat = Rollup_1_Reformat(context, df_Rollup_1)
    val df_Rollup            = Rollup(context,            df_Rollup_1_Reformat)
    val df_Rollup_Reformat   = Rollup_Reformat(context,   df_Rollup)
    val df_Reformat          = Reformat(context,          df_Rollup_Reformat)
    val df_Reformat_11       = Reformat_11(context,       df_Reformat)
    df_Reformat_11
  }

}
