package io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_placement_video_attributes_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("id").cast(IntegerType).as("id"),
      col("supports_skippable").cast(IntegerType).as("supports_skippable"),
      col("max_duration_secs").cast(IntegerType).as("max_duration_secs"),
      col("max_ad_duration_secs").cast(IntegerType).as("max_ad_duration_secs"),
      col("maximum_number_ads").cast(IntegerType).as("maximum_number_ads"),
      col("start_delay_secs").cast(IntegerType).as("start_delay_secs"),
      col("playback_method").cast(IntegerType).as("playback_method"),
      col("video_context").cast(IntegerType).as("video_context"),
      col("is_mediated").cast(IntegerType).as("is_mediated"),
      col("skip_offset").cast(IntegerType).as("skip_offset")
    )

}
