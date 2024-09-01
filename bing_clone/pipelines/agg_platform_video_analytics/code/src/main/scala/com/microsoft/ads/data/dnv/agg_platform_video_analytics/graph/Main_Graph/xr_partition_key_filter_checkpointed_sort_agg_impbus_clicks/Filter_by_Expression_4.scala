package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_by_Expression_4 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      is_not_null(col("auction_id_64"))
        .and(col("auction_id_64") =!= lit(0))
        .and(is_not_null(col("media_type")))
        .and(col("media_type").isin(64, 70))
    )

}
