package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object temp_output3 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("auction_id_64", LongType, true),
            StructField("agg_platform_video_requests_tag_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_requests_creative_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_video_events_creative_id", IntegerType, true),
            StructField("agg_platform_video_impressions_creative_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_creative_id",         IntegerType, true),
            StructField("agg_dw_pixels_creative_id",         IntegerType, true),
            StructField("agg_impbus_clicks_creative_id",     IntegerType, true),
            StructField("agg_dw_video_events_advertiser_id", IntegerType, true),
            StructField("agg_platform_video_impressions_advertiser_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_advertiser_id",     IntegerType, true),
            StructField("agg_dw_pixels_advertiser_id",     IntegerType, true),
            StructField("agg_impbus_clicks_advertiser_id", IntegerType, true),
            StructField("agg_dw_video_events_buyer_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_impressions_buyer_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_buyer_member_id",     IntegerType, true),
            StructField("agg_dw_pixels_buyer_member_id",     IntegerType, true),
            StructField("agg_impbus_clicks_buyer_member_id", IntegerType, true),
            StructField("agg_platform_video_requests_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_video_events_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_impressions_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_seller_member_id", IntegerType, true),
            StructField("agg_dw_pixels_seller_member_id", IntegerType, true),
            StructField("agg_impbus_clicks_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("f_is_buy_side_imp_agg_dw_pixels_imp_type",
                        IntegerType,
                        false
            ),
            StructField("f_is_buy_side_imp_imp_type_request_imp_type",
                        IntegerType,
                        false
            ),
            StructField(
              "f_is_buy_side_imp_agg_platform_video_impressions_imp_type",
              IntegerType,
              false
            ),
            StructField("f_is_buy_side_imp_agg_dw_clicks_imp_type",
                        IntegerType,
                        false
            ),
            StructField("f_calc_derived_fields", IntegerType, true),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP1",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP2",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP3",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP4",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP5",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP6",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_creative_media_subtype_pb_LOOKUP",
              StructType(
                Array(
                  StructField("id",                      IntegerType, true),
                  StructField("media_subtype",           IntegerType, true),
                  StructField("advertiser_id",           IntegerType, true),
                  StructField("duration_ms",             IntegerType, true),
                  StructField("vast_type_id",            IntegerType, true),
                  StructField("minimum_vast_version_id", IntegerType, true),
                  StructField("is_skippable",            IntegerType, true),
                  StructField("framework_ids",           StringType,  true)
                )
              ),
              true
            ),
            StructField(
              "_sup_placement_video_attributes_pb_LOOKUP",
              StructType(
                Array(
                  StructField("id",                   IntegerType, true),
                  StructField("supports_skippable",   IntegerType, true),
                  StructField("max_duration_secs",    IntegerType, true),
                  StructField("max_ad_duration_secs", IntegerType, true),
                  StructField("maximum_number_ads",   IntegerType, true),
                  StructField("start_delay_secs",     IntegerType, true),
                  StructField("playback_method",      IntegerType, true),
                  StructField("video_context",        IntegerType, true),
                  StructField("is_mediated",          IntegerType, true),
                  StructField("skip_offset",          IntegerType, true)
                )
              ),
              true
            )
          )
        )
      )
      .load("asd")

}
