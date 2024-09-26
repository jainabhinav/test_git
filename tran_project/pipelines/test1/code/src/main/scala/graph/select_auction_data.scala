package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object select_auction_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("buyer_member_id"),
      col("seller_member_id"),
      col("curator_member_id"),
      col("member_id"),
      col("advertiser_id"),
      col("publisher_id"),
      col("bidder_id"),
      col("width"),
      col("height"),
      col("site_id"),
      col("tag_id"),
      when(col("geo_country") === lit("--"), lit(null).cast(StringType))
        .otherwise(col("geo_country"))
        .as("geo_country"),
      col("brand_id"),
      when(col("site_domain") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("site_domain"))
        .as("site_domain"),
      col("application_id"),
      when(col("device_type") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("device_type"))
        .as("device_type"),
      col("insertion_order_id"),
      col("media_type"),
      col("curated_deal_id"),
      col("curated_deal_type"),
      col("seller_deal_id"),
      col("seller_deal_type"),
      col("campaign_group_id"),
      col("campaign_group_type_id"),
      col("fx_rate_snapshot_id"),
      when(col("is_curated") === lit(false), lit(null).cast(BooleanType))
        .otherwise(col("is_curated"))
        .as("is_curated"),
      col("video_context"),
      col("view_result"),
      when(col("view_detection_enabled") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("view_detection_enabled")).as("view_detection_enabled"),
      col("viewdef_definition_id"),
      col("viewdef_viewable"),
      when(col("view_measurable") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("view_measurable"))
        .as("view_measurable"),
      when(col("viewable") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("viewable"))
        .as("viewable"),
      col("total_data_costs_microcents"),
      col("total_cost_microcents"),
      col("total_partner_fees_microcents"),
      col("net_media_cost_microcents"),
      col("gross_revenue_microcents"),
      col("total_tech_fees_microcents"),
      col("targeted_segment_details"),
      col("excluded_targeted_segment_details"),
      col("supply_type"),
      col("vp_expose_domains"),
      col("inventory_url_id"),
      col("curator_margin_microcents"),
      col("is_curator_margin_media_cost_dependent"),
      col("curator_margin_type"),
      col("bidder_seat_id"),
      col("personal_data"),
      col("user_tz_offset"),
      when(col("region") === lit("--"), lit(null).cast(StringType))
        .otherwise(col("region"))
        .as("region"),
      when(col("dma") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("dma"))
        .as("dma"),
      col("city"),
      when(col("postal_code") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("postal_code"))
        .as("postal_code"),
      col("mobile_app_instance_id"),
      col("creative_id"),
      col("truncate_ip"),
      when(col("vp_bitmap") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("vp_bitmap"))
        .as("vp_bitmap"),
      when(col("gdpr_consent_string") === lit(""), lit(null).cast(StringType))
        .otherwise(col("gdpr_consent_string"))
        .as("gdpr_consent_string"),
      col("anonymized_user_info"),
      col("view_non_measurable_reason"),
      col("operating_system"),
      col("browser"),
      col("language"),
      col("device_id"),
      col("data_costs"),
      col("crossdevice_graph_cost"),
      col("personal_identifiers"),
      col("crossdevice_group_anon"),
      col("crossdevice_graph_membership"),
      col("has_crossdevice_reach_extension"),
      col("targeted_crossdevice_graph_id"),
      when(col("curated_line_item_currency") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("curated_line_item_currency"))
        .as("curated_line_item_currency"),
      col("split_id"),
      when(col("bidding_host_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("bidding_host_id"))
        .as("bidding_host_id"),
      col("buyer_dpvp_bitmap"),
      col("seller_dpvp_bitmap"),
      col("external_campaign_id"),
      col("external_bidrequest_imp_id"),
      col("external_bidrequest_id"),
      col("postal_code_ext_id")
    )

}
