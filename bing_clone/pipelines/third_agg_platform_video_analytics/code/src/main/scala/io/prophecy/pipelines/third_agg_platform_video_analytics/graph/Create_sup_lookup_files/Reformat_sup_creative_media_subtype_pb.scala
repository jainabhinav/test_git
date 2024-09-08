package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_sup_creative_media_subtype_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("id").cast(IntegerType).as("id"),
      col("media_subtype").cast(IntegerType).as("media_subtype"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("duration_ms").cast(IntegerType).as("duration_ms"),
      col("vast_type_id").cast(IntegerType).as("vast_type_id"),
      col("minimum_vast_version_id")
        .cast(IntegerType)
        .as("minimum_vast_version_id"),
      col("is_skippable").cast(IntegerType).as("is_skippable"),
      col("framework_ids")
    )

}
