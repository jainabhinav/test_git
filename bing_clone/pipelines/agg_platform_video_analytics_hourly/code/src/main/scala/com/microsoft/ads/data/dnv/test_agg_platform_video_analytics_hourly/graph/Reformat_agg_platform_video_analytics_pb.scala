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

object Reformat_agg_platform_video_analytics_pb {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      key(context).as("key"),
      value(context).as("value"),
      col("pod_id_64").cast(LongType).as("pod_id_64"),
      array(col("pod_id_64").cast(LongType)).as("pod_id_64_vector")
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
      coalesce(col("seller_member_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("seller_member_id"),
      coalesce(col("call_type"),                      lit("---")).as("call_type"),
      coalesce(col("publisher_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("publisher_id"),
      coalesce(col("site_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("site_id"),
      coalesce(col("tag_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("tag_id"),
      coalesce(col("browser").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("browser_id"),
      coalesce(col("application_id"),                     lit("---")).as("application_id"),
      coalesce(col("inventory_url_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("inventory_url_id"),
      coalesce(col("video_context").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("video_context"),
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
      coalesce(col("content_network_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_network_id"),
      coalesce(col("content_language_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_language_id"),
      coalesce(col("content_genre_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_genre_id"),
      coalesce(col("content_program_type_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_program_type_id"),
      coalesce(col("content_rating_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_rating_id"),
      coalesce(col("content_delivery_type_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("content_delivery_type_id"),
      coalesce(col("geo_country"), lit("--")).as("geo_country"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("billing_currency")
        .as("billing_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("billing_exchange_rate")
        .cast(DoubleType)
        .as("billing_exchange_rate"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("member_currency")
        .as("member_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("member_exchange_rate")
        .cast(DoubleType)
        .as("member_exchange_rate"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("publisher_currency")
        .as("publisher_currency"),
      col("in_f_get_agg_platform_video_analytics_hourly_pb_currency")
        .getField("publisher_exchange_rate")
        .cast(DoubleType)
        .as("publisher_exchange_rate"),
      coalesce(col("device_type").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("device_type"),
      coalesce(col("supply_type").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("supply_type"),
      coalesce(col("language").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("language_id"),
      coalesce(col("player_width").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("player_width"),
      coalesce(col("player_height").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("player_height"),
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
      coalesce(col("city").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("city"),
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
      coalesce(col("operating_system_id").cast(IntegerType), lit(1))
        .cast(IntegerType)
        .as("operating_system_id"),
      coalesce(col("operating_system_family_id").cast(IntegerType), lit(1))
        .cast(IntegerType)
        .as("operating_system_family_id"),
      coalesce(col("max_ad_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_ad_duration"),
      coalesce(col("min_ad_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("min_ad_duration"),
      coalesce(col("max_number_ads").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_number_ads"),
      coalesce(col("max_duration").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("max_duration"),
      coalesce(col("placement_set_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("placement_set_id"),
      when(coalesce(col("slot_type").cast(IntegerType), lit(0)) > lit(0),
           lit(1).cast(BooleanType)
      ).otherwise(lit(0).cast(BooleanType)).as("pod_has_bumpers"),
      coalesce(col("content_duration_secs").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("video_content_duration"),
      coalesce(col("fallback_ad_index").cast(IntegerType), lit(-1))
        .cast(IntegerType)
        .as("fallback_ad_index"),
      lit(null).cast(BooleanType).as("pod_has_imps"),
      lit(null).cast(BooleanType).as("pod_has_responses"),
      coalesce(col("region_id").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("region_id"),
      lit(null).cast(BooleanType).as("pod_has_sold_imps"),
      lit(null).cast(IntegerType).as("pod_outcome"),
      lit(null).cast(StringType).as("pod_dh")
    )
  }

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(col("reseller_revenue_dollars"), lit(0))
        .cast(DoubleType)
        .as("reseller_revenue_dollars"),
      coalesce(col("booked_revenue_dollars"), lit(0))
        .cast(DoubleType)
        .as("booked_revenue_dollars"),
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
      coalesce(col("ad_requests").cast(LongType), lit(0))
        .cast(LongType)
        .as("ad_requests"),
      coalesce(col("ad_responses").cast(LongType), lit(0))
        .cast(LongType)
        .as("ad_responses"),
      lit(null).cast(LongType).as("pod_count"),
      lit(null).cast(LongType).as("unmatched_min_slot_opportunities"),
      lit(null).cast(LongType).as("pod_no_responses"),
      lit(null).cast(LongType).as("pod_no_imps"),
      when(coalesce(col("ad_requests").cast(LongType), lit(0)) > lit(0),
           coalesce(col("max_ad_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("request_seconds"),
      when(coalesce(col("ad_responses").cast(LongType), lit(0)) > lit(0),
           coalesce(col("creative_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("responses_seconds"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(6))
          .and(coalesce(col("imps").cast(LongType), lit(0)) > lit(0))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("creative_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("impression_seconds_resold"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(5))
          .and(coalesce(col("imps").cast(LongType), lit(0)) > lit(0))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("creative_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("impression_seconds_kept"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) >= lit(1))
          .and(coalesce(col("imp_type").cast(IntegerType), lit(0)) <= lit(4))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(8))
          .and(coalesce(col("imps").cast(LongType), lit(0)) > lit(0))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("creative_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("impression_seconds_unsold"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(6))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("imps").cast(LongType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("imps_resold"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(5))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("imps").cast(LongType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("imps_kept"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) >= lit(1))
          .and(coalesce(col("imp_type").cast(IntegerType), lit(0)) <= lit(4))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(8))
          .and(coalesce(col("imps").cast(LongType), lit(0)) > lit(0))
          .and(coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(0)),
        coalesce(col("imps").cast(LongType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("imps_unsold"),
      lit(null).cast(LongType).as("min_possible_opps"),
      lit(null).cast(LongType).as("max_possible_opps"),
      lit(null).cast(LongType).as("fillable_duration"),
      lit(null).cast(LongType).as("unfilled_duration"),
      lit(null).cast(LongType).as("unfilled_duration_below_min_ad_duration"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(5))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(6))
          .and(
            (coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(1))
              .or(
                coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(2)
              )
          ),
        coalesce(col("imps").cast(LongType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("imps_sold_bumper"),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(5))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(0)) === lit(6))
          .and(coalesce(col("imps").cast(LongType), lit(0)) > lit(0))
          .and(
            (coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(1))
              .or(
                coalesce(col("slot_type").cast(IntegerType), lit(0)) === lit(2)
              )
          ),
        coalesce(col("creative_duration").cast(IntegerType), lit(0))
      ).otherwise(lit(0)).cast(LongType).as("impression_seconds_sold_bumper")
    )
  }

}
