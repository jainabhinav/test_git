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

object Join_Impbus_Clicks_with_dw_curator_impressions {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            col("left.auction_id_64") === col("right.auction_id_64"),
            "inner"
      )
      .select(
        col("left.date_time").as("date_time"),
        col("left.auction_id_64").as("auction_id_64"),
        col("left.buyer_member_id").as("buyer_member_id"),
        col("left.seller_member_id").as("seller_member_id"),
        col("left.curator_member_id").as("curator_member_id"),
        col("left.member_id").as("member_id"),
        col("left.advertiser_id").as("advertiser_id"),
        col("left.publisher_id").as("publisher_id"),
        col("left.bidder_id").as("bidder_id"),
        col("left.width").as("width"),
        col("left.height").as("height"),
        col("left.site_id").as("site_id"),
        col("left.tag_id").as("tag_id"),
        col("left.geo_country").as("geo_country"),
        col("left.brand_id").as("brand_id"),
        col("left.site_domain").as("site_domain"),
        col("left.application_id").as("application_id"),
        col("left.device_type").as("device_type"),
        col("left.insertion_order_id").as("insertion_order_id"),
        col("left.media_type").as("media_type"),
        col("left.curated_deal_id").as("curated_deal_id"),
        col("left.curated_deal_type").as("curated_deal_type"),
        col("left.seller_deal_id").as("seller_deal_id"),
        col("left.seller_deal_type").as("seller_deal_type"),
        col("left.campaign_group_id").as("campaign_group_id"),
        col("left.campaign_group_type_id").as("campaign_group_type_id"),
        col("left.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
        col("left.is_curated").cast(BooleanType).as("is_curated"),
        col("left.video_context").as("video_context"),
        col("left.viewdef_definition_id").as("viewdef_definition_id"),
        col("left.vp_expose_domains").as("vp_expose_domains"),
        col("left.inventory_url_id").as("inventory_url_id"),
        col("left.curator_margin_type").as("curator_margin_type"),
        col("left.bidder_seat_id").as("bidder_seat_id"),
        col("left.split_id").as("split_id"),
        col("left.user_tz_offset").as("user_tz_offset"),
        col("left.device_id").as("device_id"),
        col("left.operating_system").as("operating_system"),
        col("left.browser").as("browser"),
        col("left.language").as("language"),
        col("left.dma").as("dma"),
        col("left.city").as("city"),
        col("left.mobile_app_instance_id").as("mobile_app_instance_id"),
        col("left.view_result").as("view_result"),
        col("left.view_non_measurable_reason").as("view_non_measurable_reason"),
        col("left.postal_code").as("postal_code"),
        col("left.supply_type").as("supply_type"),
        col("left.postal_code_ext_id").as("postal_code_ext_id"),
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
        lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
        lit(null).cast(LongType).as("seller_dpvp_bitmap"),
        lit(null).cast(IntegerType).as("truncate_ip"),
        col("left.vp_bitmap").as("vp_bitmap"),
        col("left.creative_id").as("creative_id"),
        col("left.anonymized_user_info").as("anonymized_user_info")
      )

}
