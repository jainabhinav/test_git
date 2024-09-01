package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Filter_by_Expression_6 = Filter_by_Expression_6(context, in)
    df_Filter_by_Expression_6
  }

}
