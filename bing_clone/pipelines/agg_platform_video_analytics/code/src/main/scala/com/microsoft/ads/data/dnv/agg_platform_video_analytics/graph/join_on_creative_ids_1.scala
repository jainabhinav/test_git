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

object join_on_creative_ids_1 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame,
    in6:     DataFrame,
    in7:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in1.id") === col("in0.agg_dw_clicks_creative_id"),
            "left_outer"
      )
      .join(
        in2.as("in2"),
        col("in1.id") === col("in0.agg_platform_video_requests_creative_id"),
        "left_outer"
      )
      .join(in3.as("in3"),
            col("in3.id") === col("in0.agg_impbus_clicks_creative_id"),
            "left_outer"
      )
      .join(
        in4.as("in4"),
        col("in4.id") === col("in0.agg_platform_video_impressions_creative_id"),
        "left_outer"
      )
      .join(in5.as("in5"),
            col("in5.id") === col("in0.agg_dw_video_events_creative_id"),
            "left_outer"
      )
      .join(in6.as("in6"),
            col("in6.id") === col("in0.agg_dw_pixels_creative_id"),
            "left_outer"
      )
      .join(in7.as("in7"),
            col("in7.id") === col("in0.f_calc_derived_fields"),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.id")),
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
          is_not_null(col("in2.id")),
          struct(
            col("in2.id").as("id"),
            col("in2.media_subtype").as("media_subtype"),
            col("in2.advertiser_id").as("advertiser_id"),
            col("in2.duration_ms").as("duration_ms"),
            col("in2.vast_type_id").as("vast_type_id"),
            col("in2.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in2.is_skippable").as("is_skippable"),
            col("in2.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP1"),
        when(
          is_not_null(col("in3.id")),
          struct(
            col("in3.id").as("id"),
            col("in3.media_subtype").as("media_subtype"),
            col("in3.advertiser_id").as("advertiser_id"),
            col("in3.duration_ms").as("duration_ms"),
            col("in3.vast_type_id").as("vast_type_id"),
            col("in3.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in3.is_skippable").as("is_skippable"),
            col("in3.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP6"),
        when(
          is_not_null(col("in4.id")),
          struct(
            col("in4.id").as("id"),
            col("in4.media_subtype").as("media_subtype"),
            col("in4.advertiser_id").as("advertiser_id"),
            col("in4.duration_ms").as("duration_ms"),
            col("in4.vast_type_id").as("vast_type_id"),
            col("in4.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in4.is_skippable").as("is_skippable"),
            col("in4.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP3"),
        when(
          is_not_null(col("in5.id")),
          struct(
            col("in5.id").as("id"),
            col("in5.media_subtype").as("media_subtype"),
            col("in5.advertiser_id").as("advertiser_id"),
            col("in5.duration_ms").as("duration_ms"),
            col("in5.vast_type_id").as("vast_type_id"),
            col("in5.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in5.is_skippable").as("is_skippable"),
            col("in5.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP2"),
        when(
          is_not_null(col("in6.id")),
          struct(
            col("in6.id").as("id"),
            col("in6.media_subtype").as("media_subtype"),
            col("in6.advertiser_id").as("advertiser_id"),
            col("in6.duration_ms").as("duration_ms"),
            col("in6.vast_type_id").as("vast_type_id"),
            col("in6.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in6.is_skippable").as("is_skippable"),
            col("in6.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP5"),
        when(
          is_not_null(col("in7.id")),
          struct(
            col("in7.id").as("id"),
            col("in7.media_subtype").as("media_subtype"),
            col("in7.advertiser_id").as("advertiser_id"),
            col("in7.duration_ms").as("duration_ms"),
            col("in7.vast_type_id").as("vast_type_id"),
            col("in7.minimum_vast_version_id").as("minimum_vast_version_id"),
            col("in7.is_skippable").as("is_skippable"),
            col("in7.framework_ids").as("framework_ids")
          )
        ).as("_sup_creative_media_subtype_pb_LOOKUP"),
        col("in0.*")
      )

}
