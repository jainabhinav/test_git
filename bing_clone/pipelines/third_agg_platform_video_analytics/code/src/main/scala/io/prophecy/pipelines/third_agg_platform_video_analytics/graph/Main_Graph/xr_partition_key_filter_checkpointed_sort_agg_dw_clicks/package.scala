package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_clicks.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_agg_dw_clicks {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_repartition_dataframe = repartition_dataframe(context, in)
    val df_Filter_by_Expression_5 =
      Filter_by_Expression_5(context, df_repartition_dataframe)
    df_Filter_by_Expression_5
  }

}
