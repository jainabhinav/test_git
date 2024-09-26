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

object Filter_by_Expression_agg_dw_curator_impressionsj0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1"),
        col("in0.publisher_id").cast(IntegerType) === col("in1.publisher_id"),
        "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.campaign_group_id").cast(IntegerType) === col(
              "in2.campaign_group_id"
            ),
            "left_outer"
      )
      .join(
        in3.as("in3"),
        col("in0.advertiser_id").cast(IntegerType) === col("in3.advertiser_id"),
        "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.publisher_id")),
          struct(
            col("in1.seller_member_id").as("seller_member_id"),
            col("in1.publisher_id").as("publisher_id"),
            col("in1.site_id").as("site_id"),
            col("in1.tag_id").as("tag_id")
          )
        ).as("_member_id_by_publisher_id_LOOKUP"),
        when(
          is_not_null(col("in2.campaign_group_id")),
          struct(
            col("in2.buyer_member_id").as("buyer_member_id"),
            col("in2.advertiser_id").as("advertiser_id"),
            col("in2.campaign_group_id").as("campaign_group_id"),
            col("in2.campaign_id").as("campaign_id")
          )
        ).as("_advertiser_id_by_campaign_group_id_LOOKUP"),
        when(
          is_not_null(col("in3.advertiser_id")),
          struct(
            col("in3.buyer_member_id").as("buyer_member_id"),
            col("in3.advertiser_id").as("advertiser_id"),
            col("in3.campaign_group_id").as("campaign_group_id"),
            col("in3.campaign_id").as("campaign_id")
          )
        ).as("_member_id_by_advertiser_id_LOOKUP"),
        col("in0.date_time").as("date_time"),
        col("in0.auction_id_64").as("auction_id_64"),
        col("in0.buyer_member_id").as("buyer_member_id"),
        col("in0.seller_member_id").as("seller_member_id"),
        col("in0.curator_member_id").as("curator_member_id"),
        col("in0.member_id").as("member_id"),
        col("in0.advertiser_id").as("advertiser_id"),
        col("in0.publisher_id").as("publisher_id"),
        col("in0.bidder_id").as("bidder_id"),
        col("in0.width").as("width"),
        col("in0.height").as("height"),
        col("in0.site_id").as("site_id"),
        col("in0.tag_id").as("tag_id"),
        col("in0.geo_country").as("geo_country"),
        col("in0.brand_id").as("brand_id"),
        col("in0.site_domain").as("site_domain"),
        col("in0.application_id").as("application_id"),
        col("in0.device_type").as("device_type"),
        col("in0.insertion_order_id").as("insertion_order_id"),
        col("in0.media_type").as("media_type"),
        col("in0.curated_deal_id").as("curated_deal_id"),
        col("in0.curated_deal_type").as("curated_deal_type"),
        col("in0.seller_deal_id").as("seller_deal_id"),
        col("in0.seller_deal_type").as("seller_deal_type"),
        col("in0.campaign_group_id").as("campaign_group_id"),
        col("in0.campaign_group_type_id").as("campaign_group_type_id"),
        col("in0.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
        col("in0.is_curated").as("is_curated"),
        col("in0.video_context").as("video_context"),
        col("in0.view_result").as("view_result"),
        col("in0.view_detection_enabled").as("view_detection_enabled"),
        col("in0.viewdef_definition_id").as("viewdef_definition_id"),
        col("in0.viewdef_viewable").as("viewdef_viewable"),
        col("in0.view_measurable").as("view_measurable"),
        col("in0.viewable").as("viewable"),
        col("in0.total_data_costs_microcents")
          .as("total_data_costs_microcents"),
        col("in0.total_cost_microcents").as("total_cost_microcents"),
        col("in0.total_partner_fees_microcents")
          .as("total_partner_fees_microcents"),
        col("in0.net_media_cost_microcents").as("net_media_cost_microcents"),
        col("in0.gross_revenue_microcents").as("gross_revenue_microcents"),
        col("in0.total_tech_fees_microcents").as("total_tech_fees_microcents"),
        col("in0.targeted_segment_details").as("targeted_segment_details"),
        col("in0.excluded_targeted_segment_details")
          .as("excluded_targeted_segment_details"),
        col("in0.supply_type").as("supply_type"),
        col("in0.vp_expose_domains").as("vp_expose_domains"),
        col("in0.inventory_url_id").as("inventory_url_id"),
        col("in0.curator_margin_microcents").as("curator_margin_microcents"),
        col("in0.is_curator_margin_media_cost_dependent")
          .as("is_curator_margin_media_cost_dependent"),
        col("in0.curator_margin_type").as("curator_margin_type"),
        col("in0.bidder_seat_id").as("bidder_seat_id"),
        col("in0.personal_data").as("personal_data"),
        col("in0.user_tz_offset").as("user_tz_offset"),
        col("in0.region").as("region"),
        col("in0.dma").as("dma"),
        col("in0.city").as("city"),
        col("in0.postal_code").as("postal_code"),
        col("in0.mobile_app_instance_id").as("mobile_app_instance_id"),
        col("in0.creative_id").as("creative_id"),
        col("in0.truncate_ip").as("truncate_ip"),
        col("in0.vp_bitmap").as("vp_bitmap"),
        col("in0.gdpr_consent_string").as("gdpr_consent_string"),
        col("in0.anonymized_user_info").as("anonymized_user_info"),
        col("in0.view_non_measurable_reason").as("view_non_measurable_reason"),
        col("in0.operating_system").as("operating_system"),
        col("in0.browser").as("browser"),
        col("in0.language").as("language"),
        col("in0.device_id").as("device_id"),
        col("in0.data_costs").as("data_costs"),
        col("in0.crossdevice_graph_cost").as("crossdevice_graph_cost"),
        col("in0.personal_identifiers").as("personal_identifiers"),
        col("in0.crossdevice_group_anon").as("crossdevice_group_anon"),
        col("in0.crossdevice_graph_membership")
          .as("crossdevice_graph_membership"),
        col("in0.has_crossdevice_reach_extension")
          .as("has_crossdevice_reach_extension"),
        col("in0.targeted_crossdevice_graph_id")
          .as("targeted_crossdevice_graph_id"),
        col("in0.curated_line_item_currency").as("curated_line_item_currency"),
        col("in0.split_id").as("split_id"),
        col("in0.bidding_host_id").as("bidding_host_id"),
        col("in0.buyer_dpvp_bitmap").as("buyer_dpvp_bitmap"),
        col("in0.seller_dpvp_bitmap").as("seller_dpvp_bitmap"),
        col("in0.external_campaign_id").as("external_campaign_id"),
        col("in0.external_bidrequest_imp_id").as("external_bidrequest_imp_id"),
        col("in0.external_bidrequest_id").as("external_bidrequest_id"),
        col("in0.postal_code_ext_id").as("postal_code_ext_id")
      )

}
