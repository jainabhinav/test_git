package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_aggDwClicktrackerPb {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            col("left.auction_id_64") === col("right.auction_id_64"),
            "inner"
      )
      .select(
        col("left.date_time").as("date_time"),
        col("right.auction_id_64").as("auction_id_64"),
        col("left.user_id_64").as("user_id_64"),
        col("left.tracker_id").as("tracker_id"),
        col("left.tag_id").as("tag_id"),
        lit(0).cast(IntegerType).as("venue_id"),
        lit(0).cast(IntegerType).as("inventory_source_id"),
        lit(0).cast(IntegerType).as("session_frequency"),
        lit(0).cast(IntegerType).as("width"),
        lit(0).cast(IntegerType).as("height"),
        col("left.geo_country").as("geo_country"),
        col("left.geo_region").as("geo_region"),
        lit("u").as("gender"),
        lit(0).cast(IntegerType).as("age"),
        col("left.member_id").cast(IntegerType).as("seller_member_id"),
        col("left.member_id").cast(IntegerType).as("buyer_member_id"),
        lit(0).cast(IntegerType).as("creative_id"),
        col("left.seller_currency").as("seller_currency"),
        lit("").as("buyer_currency"),
        col("right.advertiser_id").as("advertiser_id"),
        col("right.campaign_group_id").as("campaign_group_id"),
        lit(0).cast(IntegerType).as("campaign_id"),
        lit(0).cast(IntegerType).as("creative_freq"),
        lit(0).cast(IntegerType).as("creative_rec"),
        lit(0).cast(IntegerType).as("is_learn"),
        lit(0).cast(IntegerType).as("is_remarketing"),
        lit(0).cast(IntegerType).as("advertiser_frequency"),
        lit(0).cast(IntegerType).as("advertiser_recency"),
        col("left.user_group_id").as("user_group_id"),
        lit(0).cast(IntegerType).as("camp_dp_id"),
        lit(0).cast(IntegerType).as("media_buy_id"),
        lit(0).cast(IntegerType).as("brand_id"),
        lit(0).cast(IntegerType).as("is_appnexus_cleared"),
        lit(null).cast(DoubleType).as("clear_fees"),
        col("right.media_buy_rev_share_pct").as("media_buy_rev_share_pct"),
        lit(0).cast(DoubleType).as("revenue_value"),
        col("right.pricing_type").as("pricing_type"),
        col("left.site_id").as("site_id"),
        col("left.content_category_id").as("content_category_id"),
        col("left.datacenter_id").as("datacenter_id"),
        lit(0).cast(IntegerType).as("fold_position"),
        lit(0).cast(IntegerType).as("external_inv_id"),
        lit(0).cast(DoubleType).as("cadence_modifier"),
        lit("").as("predict_type"),
        lit(0).cast(DoubleType).as("predict_goal"),
        lit(10).cast(IntegerType).as("imp_type"),
        col("right.advertiser_currency").as("advertiser_currency"),
        col("right.advertiser_exchange_rate").as("advertiser_exchange_rate"),
        col("left.ip_address").as("ip_address"),
        col("left.pub_rule_id").as("pub_rule_id"),
        col("left.publisher_id").as("publisher_id"),
        col("right.insertion_order_id").as("insertion_order_id"),
        lit(0).cast(IntegerType).as("predict_type_rev"),
        lit(0).cast(IntegerType).as("predict_type_goal"),
        lit(0).cast(IntegerType).as("predict_type_cost"),
        col("right.revenue_info.booked_revenue_dollars")
          .cast(DoubleType)
          .as("booked_revenue_dollars"),
        col("right.revenue_info.booked_revenue_adv_curr")
          .cast(DoubleType)
          .as("booked_revenue_adv_curr"),
        col("right.commission_cpm").as("commission_cpm"),
        col("right.commission_revshare").as("commission_revshare"),
        lit(0).cast(DoubleType).as("serving_fees_revshare"),
        col("left.user_tz_offset").as("user_tz_offset"),
        lit(0).cast(IntegerType).as("media_type"),
        col("left.operating_system").as("operating_system"),
        col("left.browser").as("browser"),
        col("left.language").as("language"),
        col("left.seller_currency").as("publisher_currency"),
        col("left.seller_exchange_rate")
          .cast(DoubleType)
          .as("publisher_exchange_rate"),
        f_get_media_cost_dollars_cpm(
          lit(10),
          lit(0),
          lit(0),
          col("right.media_buy_cost"),
          col("right.media_buy_rev_share_pct"),
          lit(0),
          lit(0),
          lit(0),
          lit(0),
          col("right.commission_revshare"),
          lit(0),
          lit(0),
          lit(0),
          col("right.revenue_value")
        ).cast(DoubleType).as("media_cost_dollars_cpm"),
        col("right.media_buy_cost").as("media_buy_cost"),
        col("left.site_domain").as("site_domain"),
        col("right.payment_type").as("payment_type"),
        col("right.revenue_type").as("revenue_type"),
        lit(0).cast(IntegerType).as("bidder_id"),
        lit("").as("inv_code"),
        lit("").as("application_id"),
        lit(0).cast(IntegerType).as("is_control"),
        col("left.referral_url").as("referral_url"),
        f_create_empty_pricing_term(col("left.member_id")).as("buyer_charges"),
        f_create_empty_pricing_term(col("left.member_id")).as("seller_charges"),
        lit(1).cast(IntegerType).as("buyer_trx_event_id"),
        lit(1).cast(IntegerType).as("seller_trx_event_id"),
        f_exchange_traded_is_console_event_unit_of_trx(lit(2),
                                                       lit(10),
                                                       lit(1),
                                                       lit(1)
        ).cast(BooleanType).as("is_unit_of_trx"),
        lit(0).cast(IntegerType).as("revenue_auction_event_type"),
        col("left.operating_system_family_id").as("operating_system_family_id"),
        when(col("left.region_id") =!= lit(0), col("left.region_id"))
          .as("region_id"),
        lit(null).cast(IntegerType).as("media_company_id"),
        lit(null).cast(IntegerType).as("trade_agreement_id"),
        lit(null)
          .cast(
            StructType(
              Array(
                StructField("user_id_64",       LongType,   true),
                StructField("device_unique_id", StringType, true),
                StructField("external_uid",     StringType, true),
                StructField("ip_address",       BinaryType, true),
                StructField("crossdevice_group",
                            StructType(
                              Array(StructField("graph_id", IntegerType, true),
                                    StructField("group_id", LongType,    true)
                              )
                            ),
                            true
                ),
                StructField("latitude",                 DoubleType,  true),
                StructField("longitude",                DoubleType,  true),
                StructField("ipv6_address",             BinaryType,  true),
                StructField("subject_to_gdpr",          BooleanType, true),
                StructField("geo_country",              StringType,  true),
                StructField("gdpr_consent_string",      StringType,  true),
                StructField("preempt_ip_address",       BinaryType,  true),
                StructField("device_type",              IntegerType, true),
                StructField("device_make_id",           IntegerType, true),
                StructField("device_model_id",          IntegerType, true),
                StructField("new_user_id_64",           LongType,    true),
                StructField("is_service_provider_mode", BooleanType, true),
                StructField("is_personal_info_sale",    BooleanType, true)
              )
            )
          )
          .as("personal_data"),
        when(is_not_null(col("left.anonymized_user_info")),
             col("left.anonymized_user_info")
        ).as("anonymized_user_info"),
        col("left.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
        lit(null).cast(IntegerType).as("revenue_event_type_id"),
        lit(null).cast(IntegerType).as("billing_period_id"),
        lit(null).cast(IntegerType).as("flight_id"),
        lit(null).cast(IntegerType).as("split_id"),
        col("right.revenue_info.total_partner_fees_microcents")
          .cast(LongType)
          .as("total_partner_fees_microcents"),
        col("right.revenue_info.total_data_costs_microcents")
          .cast(LongType)
          .as("total_data_costs_microcents"),
        col("right.revenue_info.total_profit_microcents")
          .cast(LongType)
          .as("total_profit_microcents"),
        lit(null).cast(IntegerType).as("campaign_group_type_id"),
        lit(null).cast(IntegerType).as("counterparty_ruleset_type"),
        lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
        lit(null).cast(LongType).as("seller_dpvp_bitmap")
      )

}
