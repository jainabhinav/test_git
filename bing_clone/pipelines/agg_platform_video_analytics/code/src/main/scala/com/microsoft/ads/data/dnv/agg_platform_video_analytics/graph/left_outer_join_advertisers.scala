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

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        col("in1.id")
          .isin(
            col("in0.agg_dw_pixels.advertiser_id").cast(IntegerType),
            col("in0.agg_dw_video_events.advertiser_id").cast(IntegerType),
            col("in0.agg_platform_video_impressions.advertiser_id")
              .cast(IntegerType),
            col("in0.agg_dw_clicks.advertiser_id").cast(IntegerType)
          )
          .and(
            col("in1.member_id").isin(
              col("in0.agg_dw_pixels.buyer_member_id").cast(IntegerType),
              col("in0.agg_dw_pixels.seller_member_id").cast(IntegerType),
              col("in0.agg_dw_video_events.buyer_member_id").cast(IntegerType),
              col("in0.agg_dw_video_events.seller_member_id").cast(IntegerType),
              col("in0.agg_platform_video_impressions.buyer_member_id")
                .cast(IntegerType),
              col("in0.agg_platform_video_impressions.seller_member_id")
                .cast(IntegerType),
              col("in0.agg_dw_clicks.buyer_member_id").cast(IntegerType),
              col("in0.agg_dw_clicks.seller_member_id").cast(IntegerType)
            )
          ),
        "left_outer"
      )
      .select(
        when(
          (col("in0.agg_dw_pixels.advertiser_id") === col("in1.id")).and(
            when(col("in0.f_is_buy_side_imp_agg_dw_pixels_imp_type") === lit(1),
                 col("in0.agg_dw_pixels.buyer_member_id")
            ).otherwise(col("in0.agg_dw_pixels.seller_member_id")) === col(
              "in1.member_id"
            )
          ),
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
          (col("in0.agg_dw_video_events.advertiser_id") === col("in1.id")).and(
            when(
              col("in0.f_is_buy_side_imp_imp_type_request_imp_type") === lit(1),
              col("in0.agg_dw_video_events.buyer_member_id")
            ).otherwise(
              col("in0.agg_dw_video_events.seller_member_id")
            ) === col("in1.member_id")
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP5"),
        when(
          (col("in0.agg_platform_video_impressions.advertiser_id") === col(
            "in1.id"
          )).and(
            when(
              col(
                "in0.f_is_buy_side_imp_agg_platform_video_impressions_imp_type"
              ) === lit(1),
              col("in0.agg_platform_video_impressions.buyer_member_id")
            ).otherwise(
              col("in0.agg_platform_video_impressions.seller_member_id")
            ) === col("in1.member_id")
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP7"),
        when(
          (col("in0.agg_dw_clicks.advertiser_id") === col("in1.id")).and(
            when(col("in0.f_is_buy_side_imp_agg_dw_clicks_imp_type") === lit(1),
                 col("in0.agg_dw_clicks.buyer_member_id")
            ).otherwise(col("in0.agg_dw_clicks.seller_member_id")) === col(
              "in1.member_id"
            )
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP3"),
        when(
          (col("in0.agg_dw_pixels.advertiser_id") === col("in1.id")).and(
            when(col("in0.f_is_buy_side_imp_agg_dw_pixels_imp_type") === lit(0),
                 col("in0.agg_dw_pixels.buyer_member_id")
            ).otherwise(col("in0.agg_dw_pixels.seller_member_id")) === col(
              "in1.member_id"
            )
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP"),
        when(
          (col("in0.agg_dw_video_events.advertiser_id") === col("in1.id")).and(
            when(
              col("in0.f_is_buy_side_imp_imp_type_request_imp_type") === lit(0),
              col("in0.agg_dw_video_events.buyer_member_id")
            ).otherwise(
              col("in0.agg_dw_video_events.seller_member_id")
            ) === col("in1.member_id")
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP4"),
        when(
          (col("in0.agg_platform_video_impressions.advertiser_id") === col(
            "in1.id"
          )).and(
            when(
              col(
                "in0.f_is_buy_side_imp_agg_platform_video_impressions_imp_type"
              ) === lit(0),
              col("in0.agg_platform_video_impressions.buyer_member_id")
            ).otherwise(
              col("in0.agg_platform_video_impressions.seller_member_id")
            ) === col("in1.member_id")
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP6"),
        when(
          (col("in0.agg_dw_clicks.advertiser_id") === col("in1.id")).and(
            when(col("in0.f_is_buy_side_imp_agg_dw_clicks_imp_type") === lit(0),
                 col("in0.agg_dw_clicks.buyer_member_id")
            ).otherwise(col("in0.agg_dw_clicks.seller_member_id")) === col(
              "in1.member_id"
            )
          ),
          struct(
            col("in1.id").as("id"),
            col("in1.member_id").as("member_id"),
            col("in1.advertiser_default_currency")
              .as("advertiser_default_currency"),
            col("in1.is_running_political_ads").as("is_running_political_ads"),
            col("in1.name").as("name")
          )
        ).as("_sup_bidder_advertiser_pb_LOOKUP2"),
        col("in0.*")
      )

}
