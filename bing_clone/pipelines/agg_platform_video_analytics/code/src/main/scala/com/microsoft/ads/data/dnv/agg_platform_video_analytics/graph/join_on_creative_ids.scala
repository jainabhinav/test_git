package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object join_on_creative_ids {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        col("in1.id").isin(
          col("in0.agg_dw_clicks.creative_id"),
          col("in0.agg_platform_video_requests.creative_id"),
          col("in0.agg_impbus_clicks.creative_id"),
          col("in0.agg_platform_video_impressions.creative_id"),
          col("in0.agg_dw_video_events.creative_id"),
          col("in0.agg_dw_pixels.creative_id"),
          col("in0.f_calc_derived_fields")
        ),
        "inner"
      )
      .select(
        when(
          col("in0.agg_dw_clicks.creative_id") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP4"),
        when(
          col("in0.agg_platform_video_requests.creative_id") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP1"),
        when(
          col("in0.agg_impbus_clicks.creative_id") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP6"),
        when(
          col("in0.agg_platform_video_impressions.creative_id") === col(
            "in1.id"
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP3"),
        when(
          col("in0.agg_dw_video_events.creative_id") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP2"),
        when(
          col("in0.agg_dw_pixels.creative_id") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP5"),
        when(
          col("in0.f_calc_derived_fields") === col("in1.id"),
          struct(
            col("in1.id").as("id"),
            col("in1.media_subtype").as("media_subtype"),
            col("in1.advertiser_id").as("advertiser_id"),
            col("in1.duration_ms").as("duration_ms"),
            col("in1.vast_type_id").as("vast_type_id"),
            col("in1.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in1.is_skippable").as("is_skippable"),
            col("in1.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP"),
        col("in0.*")
      )

}
