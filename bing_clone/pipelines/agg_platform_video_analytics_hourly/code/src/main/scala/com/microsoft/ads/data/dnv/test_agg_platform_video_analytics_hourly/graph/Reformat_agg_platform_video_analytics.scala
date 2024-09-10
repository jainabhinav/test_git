package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_platform_video_analytics {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      key(context).as("key"),
      value(context).as("value"),
      coalesce(col("filled_number_ads").cast(IntegerType), lit(0))
        .cast(LongType)
        .as("filled_number_ads_64"),
      coalesce(col("max_slot_duration").cast(IntegerType), lit(0))
        .cast(LongType)
        .as("max_slot_duration_64"),
      coalesce(col("filled_slot_duration").cast(IntegerType), lit(0))
        .cast(LongType)
        .as("filled_slot_duration_64")
    )
  }

  def key(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      date_format(to_timestamp(concat(lit(Config.XR_BUSINESS_DATE),
                                      lit(Config.XR_BUSINESS_HOUR)
                               ),
                               "yyyyMMddHH"
                  ),
                  "yyyy-MM-dd HH:mm:ss"
      ).as("ymdh"),
      coalesce(col("imp_type").cast(IntegerType), lit(1))
        .cast(IntegerType)
        .as("imp_type"),
      coalesce(col("is_dw_buyer").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("is_dw_buyer"),
      when(
        coalesce(lookup("sup_member_attributes_pb",
                        col("seller_member_id").cast(IntegerType)
                 ).getField("is_external_supply"),
                 lit(0)
        ).cast(IntegerType) === lit(1),
        lit(0)
      ).otherwise(lit(1)).cast(IntegerType).as("is_dw_seller"),
      coalesce(col("seller_member_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("seller_member_id"),
      coalesce(col("publisher_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("publisher_id"),
      coalesce(col("site_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("site_id"),
      coalesce(col("tag_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("tag_id"),
      coalesce(col("placement_set_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("placement_set_id"),
      coalesce(col("_sup_inventory_url_pb_LOOKUP").getField("url"), lit("---"))
        .as("site_domain"),
      coalesce(col("call_type"),                   lit("---")).as("call_type"),
      coalesce(col("is_prebid").cast(BooleanType), lit(0).cast(BooleanType))
        .as("is_prebid"),
      when(size(col("allowed_media_types")) > lit(1), lit(1).cast(BooleanType))
        .otherwise(lit(0).cast(BooleanType))
        .as("is_super_auction"),
      lit(0).cast(BooleanType).as("is_mediated"),
      coalesce(col("deal_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("deal_id"),
      coalesce(
        col("_sup_api_inventory_url_content_category_LOOKUP")
          .getField("parent_category_id")
          .cast(IntegerType),
        coalesce(col("_sup_api_inventory_url_content_category_LOOKUP").getField(
                   "content_category_id"
                 ),
                 lit(0)
        ).cast(IntegerType)
      ).as("content_category_id"),
      when(
        coalesce(col("playback_method").cast(IntegerType), lit(0)) === lit(0),
        when(
          size(col("supported_playback_methods")) =!= 0,
          element_at(reverse(
                       filter(transform(col("supported_playback_methods"),
                                        ii => when(ii =!= lit(0), ii)
                              ),
                              xx => is_not_null(xx)
                       )
                     ),
                     1
          )
        ).otherwise(
          coalesce(col("playback_method").cast(IntegerType), lit(0))
            .cast(IntegerType)
        )
      ).otherwise(
          coalesce(col("playback_method").cast(IntegerType), lit(0))
            .cast(IntegerType)
        )
        .cast(IntegerType)
        .as("playback_method"),
      coalesce(col("video_context").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("video_context"),
      coalesce(col("player_width").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("player_width"),
      coalesce(col("player_height").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("player_height"),
      coalesce(col("max_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_duration"),
      coalesce(col("start_delay").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("start_delay"),
      when(
        is_not_null(col("frameworks")).cast(BooleanType),
        temp843412_UDF(lit(0), col("frameworks"))
          .getField("supports_vpaid_1") + temp843412_UDF(lit(0),
                                                         col("frameworks")
        ).getField("supports_vpaid_2")
      ).otherwise(lit(0)).cast(IntegerType).as("supports_vpaid"),
      when(
        size(col("protocols")) =!= 0,
        when(
          when(is_not_null(col("protocols")).cast(BooleanType),
               temp844044_UDF(lit(-1), col("protocols"), lit(0)).getField(
                 "valid_max_vast_version"
               )
          ).otherwise(lit(0)).cast(IntegerType) === lit(1),
          when(is_not_null(col("protocols")).cast(BooleanType),
               temp844044_UDF(lit(-1), col("protocols"), lit(0)).getField(
                 "max_vast_version"
               )
          ).otherwise(lit(-1)).cast(IntegerType) + lit(1)
        ).otherwise(lit(0))
      ).otherwise(
          when(is_not_null(col("protocols")).cast(BooleanType),
               temp844044_UDF(lit(-1), col("protocols"), lit(0))
                 .getField("max_vast_version")
          ).otherwise(lit(-1)).cast(IntegerType)
        )
        .cast(IntegerType)
        .as("max_vast_version"),
      coalesce(col("supports_skippable").cast(BooleanType),
               lit(0).cast(BooleanType)
      ).as("supports_skippable"),
      coalesce(col("min_ad_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("min_ad_duration"),
      coalesce(col("max_ad_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_ad_duration"),
      coalesce(col("buyer_member_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("buyer_member_id"),
      coalesce(col("advertiser_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("advertiser_id"),
      coalesce(col("insertion_order_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("insertion_order_id"),
      coalesce(col("campaign_group_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("campaign_group_id"),
      coalesce(col("campaign_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("campaign_id"),
      coalesce(col("bidder_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("bidder_id"),
      when(
        coalesce(col("media_type").cast(IntegerType), lit(0)) === lit(0),
        when(
          when(
            coalesce(col("media_type").cast(IntegerType), lit(0)) === lit(0),
            when(is_not_null(col("allowed_media_types")).cast(BooleanType),
                 temp676085_UDF(col("allowed_media_types"), lit(0)).getField(
                   "media_type_video"
                 )
            ).otherwise(lit(0))
          ).otherwise(lit(0)).cast(IntegerType) === lit(1),
          lit(64)
        ).when(
            when(
              coalesce(col("media_type").cast(IntegerType), lit(0)) === lit(0),
              when(is_not_null(col("allowed_media_types")).cast(BooleanType),
                   temp676085_UDF(col("allowed_media_types"), lit(0)).getField(
                     "media_type_audio"
                   )
              ).otherwise(lit(0))
            ).otherwise(lit(0)).cast(IntegerType) === lit(1),
            lit(70)
          )
          .otherwise(
            coalesce(col("media_type").cast(IntegerType), lit(0))
              .cast(IntegerType)
          )
      ).otherwise(
          coalesce(col("media_type").cast(IntegerType), lit(0))
            .cast(IntegerType)
        )
        .cast(IntegerType)
        .as("media_type"),
      coalesce(col("brand_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("brand_id"),
      coalesce(col("creative_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("creative_id"),
      coalesce(col("vast_error_code").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("vast_error_code"),
      coalesce(col("creative_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("creative_duration"),
      coalesce(col("viewdef_definition_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("viewdef_definition_id"),
      coalesce(col("companion_creative_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("companion_creative_id"),
      coalesce(col("view_detection_enabled").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("view_detection_enabled"),
      coalesce(col("device_type").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("device_type"),
      coalesce(col("supply_type").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("supply_type"),
      coalesce(col("geo_country"), lit("--")).as("geo_country"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("advertiser_currency")
        .as("advertiser_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("publisher_currency")
        .as("publisher_currency"),
      coalesce(col("max_number_ads").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_number_ads"),
      coalesce(col("application_id"),                            lit("---")).as("application_id"),
      coalesce(col("wrapper_vast_error_code").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("wrapper_vast_error_code"),
      coalesce(col("wrapper_internal_error_code").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("wrapper_internal_error_code"),
      coalesce(col("is_vpaid").cast(BooleanType), lit(0).cast(BooleanType))
        .as("is_vpaid"),
      coalesce(lookup("sup_member_attributes_pb",
                      col("seller_member_id").cast(IntegerType)
               ).getField("seller_member_group_id"),
               lit(0)
      ).cast(IntegerType).as("seller_member_group_id"),
      coalesce(col("slot_type").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("slot_type"),
      coalesce(col("ad_slot_position").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("ad_slot_position"),
      coalesce(col("_sup_inventory_url_pb_LOOKUP").getField("list"), lit(0))
        .cast(IntegerType)
        .as("url_list"),
      coalesce(col("buyer_trx_event_id").cast(IntegerType), lit(1))
        .cast(IntegerType)
        .as("buyer_payment_event_type_id"),
      coalesce(col("seller_trx_event_id").cast(IntegerType), lit(1))
        .cast(IntegerType)
        .as("seller_revenue_event_type_id"),
      coalesce(col("inventory_url_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("inventory_url_id"),
      coalesce(col("mobile_app_instance_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("mobile_app_instance_id"),
      coalesce(col("allowed_media_types_bitmap").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("allowed_media_types_bitmap"),
      coalesce(col("protocols_bitmap").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("protocols_bitmap"),
      coalesce(col("frameworks_bitmap").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("frameworks_bitmap"),
      lit(0).cast(IntegerType).as("split_id"),
      coalesce(col("campaign_group_type_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("campaign_group_type_id"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("advertiser_default_currency")
        .as("advertiser_default_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("advertiser_default_exchange_rate")
        .cast(DoubleType)
        .as("advertiser_default_exchange_rate"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("member_currency")
        .as("member_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("member_exchange_rate")
        .cast(DoubleType)
        .as("member_exchange_rate"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("billing_currency")
        .as("billing_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("billing_exchange_rate")
        .cast(DoubleType)
        .as("billing_exchange_rate"),
      lit(null).cast(IntegerType).as("fx_rate_snapshot_id"),
      coalesce(col("hb_source").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("hb_source"),
      coalesce(col("bidder_seat_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("bidder_seat_id"),
      coalesce(col("content_delivery_type_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_delivery_type_id"),
      coalesce(col("content_duration_secs").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_duration_secs"),
      coalesce(col("content_genre_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_genre_id"),
      coalesce(col("content_program_type_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_program_type_id"),
      coalesce(col("content_rating_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_rating_id"),
      coalesce(col("content_network_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_network_id"),
      coalesce(col("content_language_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_language_id"),
      coalesce(col("fallback_ad_index").cast(IntegerType), lit(-1))
        .cast(IntegerType)
        .as("fallback_ad_index")
    )
  }

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(col("booked_revenue_dollars"), lit(0))
        .cast(DoubleType)
        .as("booked_revenue_dollars"),
      coalesce(col("reseller_revenue_dollars"), lit(0))
        .cast(DoubleType)
        .as("reseller_revenue_dollars"),
      coalesce(col("buyer_media_cost_dollars"), lit(0))
        .cast(DoubleType)
        .as("media_cost_dollars"),
      coalesce(col("booked_revenue_dollars") * col("advertiser_exchange_rate"),
               lit(0)
      ).cast(DoubleType).as("booked_revenue_adv_currency"),
      coalesce(col("buyer_media_cost_dollars") * col("publisher_exchange_rate"),
               lit(0)
      ).cast(DoubleType).as("media_cost_pub_currency"),
      coalesce(col("ad_requests").cast(LongType), lit(0))
        .cast(LongType)
        .as("ad_requests"),
      coalesce(col("blacklisted_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("blacklisted_imps"),
      coalesce(col("ad_responses").cast(LongType), lit(0))
        .cast(LongType)
        .as("ad_responses"),
      coalesce(col("imps").cast(LongType),   lit(0)).cast(LongType).as("imps"),
      coalesce(col("starts").cast(LongType), lit(0))
        .cast(LongType)
        .as("starts"),
      coalesce(col("skips").cast(LongType),  lit(0)).cast(LongType).as("skips"),
      coalesce(col("errors").cast(LongType), lit(0))
        .cast(LongType)
        .as("errors"),
      coalesce(col("first_quartiles").cast(LongType), lit(0))
        .cast(LongType)
        .as("first_quartiles"),
      coalesce(col("second_quartiles").cast(LongType), lit(0))
        .cast(LongType)
        .as("second_quartiles"),
      coalesce(col("third_quartiles").cast(LongType), lit(0))
        .cast(LongType)
        .as("third_quartiles"),
      coalesce(col("completions").cast(LongType), lit(0))
        .cast(LongType)
        .as("completions"),
      coalesce(col("clicks").cast(LongType), lit(0))
        .cast(LongType)
        .as("clicks"),
      coalesce(col("post_click_conversions").cast(LongType), lit(0))
        .cast(LongType)
        .as("post_click_conversions"),
      coalesce(col("post_view_conversions").cast(LongType), lit(0))
        .cast(LongType)
        .as("post_view_conversions"),
      coalesce(col("companion_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("companion_imps"),
      coalesce(col("companion_clicks").cast(LongType), lit(0))
        .cast(LongType)
        .as("companion_clicks"),
      coalesce(col("viewed_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("viewed_imps"),
      coalesce(col("view_measurable_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("view_measurable_imps"),
      coalesce(col("viewdef_viewed_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("viewdef_viewed_imps"),
      coalesce(col("imp_bid_on").cast(IntegerType), lit(0))
        .cast(LongType)
        .as("imps_bid_on"),
      coalesce(col("num_of_bids").cast(IntegerType), lit(0))
        .cast(LongType)
        .as("num_of_bids"),
      coalesce(col("buyer_bid"),                           lit(0)).cast(DoubleType).as("buyer_bid"),
      coalesce(col("filled_number_ads").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("filled_number_ads"),
      coalesce(col("max_slot_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_slot_duration"),
      coalesce(col("filled_slot_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("filled_slot_duration"),
      coalesce(col("biddable_imps").cast(LongType), lit(0))
        .cast(LongType)
        .as("biddable_imps"),
      coalesce(col("buyer_payment_event_units").cast(LongType), lit(0))
        .cast(LongType)
        .as("buyer_payment_event_units"),
      coalesce(col("seller_revenue_event_units").cast(LongType), lit(0))
        .cast(LongType)
        .as("seller_revenue_event_units"),
      lit(null).cast(LongType).as("imp_requests")
    )
  }

}
