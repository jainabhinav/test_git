package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_dw_video_events_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object repartition_by_auction_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.repartition(col("auction_id_64"))

}
