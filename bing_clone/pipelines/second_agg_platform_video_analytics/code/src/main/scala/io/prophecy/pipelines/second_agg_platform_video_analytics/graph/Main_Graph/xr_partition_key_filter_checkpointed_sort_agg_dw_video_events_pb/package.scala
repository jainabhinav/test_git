package io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_repartition_by_auction_id = repartition_by_auction_id(context, in)
    val df_Filter_by_Expression_6 =
      Filter_by_Expression_6(context, df_repartition_by_auction_id)
    df_Filter_by_Expression_6
  }

}
