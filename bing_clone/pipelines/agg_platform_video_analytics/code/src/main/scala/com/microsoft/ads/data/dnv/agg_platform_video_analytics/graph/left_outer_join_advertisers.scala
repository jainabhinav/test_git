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

object left_outer_join_advertisers {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame,
    in6:     DataFrame,
    in7:     DataFrame,
    in8:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        (col("in0.agg_dw_pixels_advertiser_id") === col("in1.id")).and(
          when(col("in0.f_is_buy_side_imp_agg_dw_pixels_imp_type") === lit(1),
               col("in0.agg_dw_pixels_buyer_member_id")
          ).otherwise(col("in0.agg_dw_pixels_seller_member_id")) === col(
            "in1.member_id"
          )
        ),
        "left_outer"
      )
      .join(
        in2.as("in2"),
        (col("in0.agg_dw_video_events_advertiser_id") === col("in2.id")).and(
          when(
            col("in0.f_is_buy_side_imp_imp_type_request_imp_type") === lit(1),
            col("in0.agg_dw_video_events_buyer_member_id")
          ).otherwise(col("in0.agg_dw_video_events_seller_member_id")) === col(
            "in2.member_id"
          )
        ),
        "left_outer"
      )
      .join(
        in3.as("in3"),
        (col("in0.agg_platform_video_impressions_advertiser_id") === col(
          "in3.id"
        )).and(
          when(col(
                 "in0.f_is_buy_side_imp_agg_platform_video_impressions_imp_type"
               ) === lit(1),
               col("in0.agg_platform_video_impressions_buyer_member_id")
          ).otherwise(
            col("in0.agg_platform_video_impressions_seller_member_id")
          ) === col("in3.member_id")
        ),
        "left_outer"
      )
      .join(
        in4.as("in4"),
        (col("in0.agg_dw_clicks_advertiser_id") === col("in4.id")).and(
          when(col("f_is_buy_side_imp_agg_dw_clicks_imp_type") === lit(0),
               col("in0.agg_dw_clicks_buyer_member_id")
          ).otherwise(col("in0.agg_dw_clicks_seller_member_id")) === col(
            "in4.member_id"
          )
        ),
        "left_outer"
      )
      .join(
        in5.as("in5"),
        (col("in0.agg_platform_video_impressions_advertiser_id") === col(
          "in5.id"
        )).and(
          when(col(
                 "in0.f_is_buy_side_imp_agg_platform_video_impressions_imp_type"
               ) === lit(0),
               col("in0.agg_platform_video_impressions_buyer_member_id")
          ).otherwise(
            col("in0.agg_platform_video_impressions_seller_member_id")
          ) === col("in5.member_id")
        ),
        "left_outer"
      )
      .join(
        in6.as("in6"),
        (col("in0.agg_dw_video_events_advertiser_id") === col("in6.id")).and(
          when(
            col("in0.f_is_buy_side_imp_imp_type_request_imp_type") === lit(0),
            col("in0.agg_dw_video_events_buyer_member_id")
          ).otherwise(col("in0.agg_dw_video_events_seller_member_id")) === col(
            "in6.member_id"
          )
        ),
        "left_outer"
      )
      .join(
        in7.as("in7"),
        (col("in0.agg_dw_clicks_advertiser_id") === col("in7.id")).and(
          when(col("f_is_buy_side_imp_agg_dw_clicks_imp_type") === lit(1),
               col("in0.agg_dw_clicks_buyer_member_id")
          ).otherwise(col("in0.agg_dw_clicks_seller_member_id")) === col(
            "in7.member_id"
          )
        ),
        "left_outer"
      )
      .join(
        in8.as("in8"),
        (col("in0.agg_dw_pixels_advertiser_id") === col("in8.id")).and(
          when(col("in0.f_is_buy_side_imp_agg_dw_pixels_imp_type") === lit(0),
               col("in0.agg_dw_pixels_buyer_member_id")
          ).otherwise(col("in0.agg_dw_pixels_seller_member_id")) === col(
            "in8.member_id"
          )
        ),
        "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.id")).and(is_not_null(col("in1.member_id"))),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP1"),
        when(
          is_not_null(col("in2.id")).and(is_not_null(col("in2.member_id"))),
          struct(
            col("in2.id").as("id"),
            col("in2.member_id").as("member_id"),
            col("in2.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in2.is_running_political_ads").as("is_running_political_ads"),
            col("in2.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP5"),
        when(
          is_not_null(col("in3.id")).and(is_not_null(col("in3.member_id"))),
          struct(
            col("in3.id").as("id"),
            col("in3.member_id").as("member_id"),
            col("in3.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in3.is_running_political_ads").as("is_running_political_ads"),
            col("in3.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP7"),
        when(
          is_not_null(col("in4.id")).and(is_not_null(col("in4.member_id"))),
          struct(
            col("in4.id").as("id"),
            col("in4.member_id").as("member_id"),
            col("in4.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in4.is_running_political_ads").as("is_running_political_ads"),
            col("in4.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP3"),
        when(
          is_not_null(col("in5.id")).and(is_not_null(col("in5.member_id"))),
          struct(
            col("in5.id").as("id"),
            col("in5.member_id").as("member_id"),
            col("in5.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in5.is_running_political_ads").as("is_running_political_ads"),
            col("in5.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP"),
        when(
          is_not_null(col("in6.id")).and(is_not_null(col("in6.member_id"))),
          struct(
            col("in6.id").as("id"),
            col("in6.member_id").as("member_id"),
            col("in6.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in6.is_running_political_ads").as("is_running_political_ads"),
            col("in6.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP4"),
        when(
          is_not_null(col("in7.id")).and(is_not_null(col("in7.member_id"))),
          struct(
            col("in7.id").as("id"),
            col("in7.member_id").as("member_id"),
            col("in7.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in7.is_running_political_ads").as("is_running_political_ads"),
            col("in7.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP6"),
        when(
          is_not_null(col("in8.id")).and(is_not_null(col("in8.member_id"))),
          struct(
            col("in8.id").as("id"),
            col("in8.member_id").as("member_id"),
            col("in8.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in8.is_running_political_ads").as("is_running_political_ads"),
            col("in8.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP2"),
        col("in0.*")
      )

}
