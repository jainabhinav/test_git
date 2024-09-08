package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_platform_video_requests

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_platform_video_requests.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Partition_by_Key_UnionAll {

  def apply(context: Context, in3: DataFrame, in2: DataFrame): DataFrame =
    List(in3, in2).flatMap(Option(_)).reduce(_.unionAll(_))

}
