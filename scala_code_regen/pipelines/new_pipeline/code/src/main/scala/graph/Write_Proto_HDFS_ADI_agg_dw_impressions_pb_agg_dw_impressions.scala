package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_ADI_agg_dw_impressions_pb_agg_dw_impressions {

  def apply(context: Context, in: DataFrame): Unit = {
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    try {
      val schema = Some(
        """type fixed32_t = unsigned little endian integer(4);
type fixed64_t = unsigned little endian integer(8);
type sfixed32_t = little endian integer(4);
type sfixed64_t = little endian integer(8);
type real32_t = little endian ieee real(4);
type real64_t = little endian ieee real(8);
type bool_t = unsigned integer(1);
type string_t = utf8 string(unsigned little endian integer(4));
type bytes_t = void(unsigned little endian integer(4));
type enum_t = little endian integer(4);
type length_t = unsigned little endian integer(4);
type campaign_group_model =
record
  sfixed32_t model_type = NULL;
  sfixed32_t model_id = NULL;
  string_t leaf_code = NULL;
  sfixed32_t origin = NULL;
  sfixed32_t experiment = NULL;
  real32_t value = NULL;
end;
type crossdevice_group_anonymized =
record
  sfixed32_t graph_id = NULL;
  bytes_t group_id = NULL;
end;
type data_cost =
record
  sfixed32_t data_member_id = NULL;
  real64_t cost = NULL;
  sfixed32_t[length_t] used_segments = NULL;
  real64_t cost_pct = NULL;
end;
type log_anonymized_user_info =
record
  bytes_t user_id = NULL;
end;
type log_product_ads =
record
  sfixed32_t product_feed_id = NULL;
  sfixed32_t item_selection_strategy_id = NULL;
  string_t product_uuid = NULL;
end;
type pricing_term =
record
  sfixed32_t term_id = NULL;
  real64_t amount = NULL;
  real64_t rate = NULL;
  bool_t is_deduction = NULL;
  bool_t is_media_cost_dependent = NULL;
  sfixed32_t data_member_id = NULL;
end;
type member_pricing_term =
record
  sfixed32_t rate_card_id = NULL;
  sfixed32_t member_id = NULL;
  bool_t is_dw = NULL;
  pricing_term[length_t] pricing_terms = NULL;
  sfixed32_t fx_margin_rate_id = NULL;
  sfixed32_t marketplace_owner_id = NULL;
  sfixed32_t virtual_marketplace_id = NULL;
  bool_t amino_enabled = NULL;
end;
type crossdevice_group =
record
  sfixed32_t graph_id = NULL;
  sfixed64_t group_id = NULL;
end;
type personal_data =
record
  fixed64_t user_id_64 = NULL;
  string_t device_unique_id = NULL;
  string_t external_uid = NULL;
  bytes_t ip_address = NULL;
  crossdevice_group crossdevice_group = NULL;
  real64_t latitude = NULL;
  real64_t longitude = NULL;
  bytes_t ipv6_address = NULL;
  bool_t subject_to_gdpr = NULL;
  string_t geo_country = NULL;
  string_t gdpr_consent_string = NULL;
  bytes_t preempt_ip_address = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t device_make_id = NULL;
  sfixed32_t device_model_id = NULL;
  fixed64_t new_user_id_64 = NULL;
  bool_t is_service_provider_mode = NULL;
  bool_t is_personal_info_sale = NULL;
end;
type tag_size =
record
  sfixed32_t width;
  sfixed32_t height;
end;
type crossdevice_graph_cost =
record
  sfixed32_t graph_provider_member_id = NULL;
  real64_t cost_cpm_usd = NULL;
end;
type targeted_segment_details =
record
  fixed32_t segment_id = NULL;
  fixed32_t last_seen_min = NULL;
end;
type excluded_targeted_segment_details =
record
  fixed32_t segment_id = NULL;
end;
type log_personal_identifier =
record
  fixed32_t identity_type;
  string_t identity_value = NULL;
end;
type targeted_segment_details_by_id_type =
record
  fixed32_t identity_type = NULL;
  targeted_segment_details[length_t] targeted_segment_details = NULL;
end;
type offline_attribution =
record
  fixed32_t provider_member_id = NULL;
  real64_t cost_usd_cpm = NULL;
end;
type agg_dw_impressions =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tag_id = NULL;
  sfixed32_t venue_id = NULL;
  sfixed32_t inventory_source_id = NULL;
  sfixed32_t session_frequency = NULL;
  string_t site_domain = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  string_t geo_country = NULL;
  string_t geo_region = NULL;
  string_t gender = NULL;
  sfixed32_t age = NULL;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t creative_id = NULL;
  string_t seller_currency = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_bid = NULL;
  real64_t buyer_spend = NULL;
  real64_t ecp = NULL;
  real64_t reserve_price = NULL;
  sfixed32_t advertiser_id = NULL;
  sfixed32_t campaign_group_id = NULL;
  sfixed32_t campaign_id = NULL;
  sfixed32_t creative_freq = NULL;
  sfixed32_t creative_rec = NULL;
  sfixed32_t is_learn = NULL;
  sfixed32_t is_remarketing = NULL;
  sfixed32_t advertiser_frequency = NULL;
  sfixed32_t advertiser_recency = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t camp_dp_id = NULL;
  sfixed32_t media_buy_id = NULL;
  real64_t media_buy_cost = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t cleared_direct = NULL;
  real64_t clear_fees = NULL;
  real64_t media_buy_rev_share_pct = NULL;
  real64_t revenue_value = NULL;
  string_t pricing_type = NULL;
  sfixed32_t can_convert = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t is_control = NULL;
  real64_t control_pct = NULL;
  sfixed32_t control_creative_id = NULL;
  real64_t predicted_cpm = NULL;
  real64_t actual_bid = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t content_category_id = NULL;
  real64_t auction_service_fees = NULL;
  real64_t discrepancy_allowance = NULL;
  real64_t forex_allowance = NULL;
  real64_t creative_overage_fees = NULL;
  sfixed32_t fold_position = NULL;
  sfixed32_t external_inv_id = NULL;
  real64_t cadence_modifier = NULL;
  enum_t imp_type;
  string_t advertiser_currency = NULL;
  real64_t advertiser_exchange_rate = NULL;
  string_t ip_address = NULL;
  sfixed32_t publisher_id = NULL;
  real64_t auction_service_deduction = NULL;
  sfixed32_t insertion_order_id = NULL;
  sfixed32_t predict_type_rev = NULL;
  sfixed32_t predict_type_goal = NULL;
  sfixed32_t predict_type_cost = NULL;
  real64_t booked_revenue_dollars = NULL;
  real64_t booked_revenue_adv_curr = NULL;
  real64_t commission_cpm = NULL;
  real64_t commission_revshare = NULL;
  real64_t serving_fees_cpm = NULL;
  real64_t serving_fees_revshare = NULL;
  sfixed32_t user_tz_offset = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t language = NULL;
  string_t publisher_currency = NULL;
  real64_t publisher_exchange_rate = NULL;
  real64_t media_cost_dollars_cpm = NULL;
  enum_t payment_type = NULL;
  enum_t revenue_type = NULL;
  real64_t seller_revenue_cpm = NULL;
  sfixed32_t bidder_id = NULL;
  string_t inv_code = NULL;
  string_t application_id = NULL;
  real64_t shadow_price = NULL;
  real64_t eap = NULL;
  sfixed32_t datacenter_id = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t vp_expose_categories = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  sfixed32_t vp_expose_tag = NULL;
  sfixed32_t vp_expose_age = NULL;
  sfixed32_t vp_expose_gender = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t audit_type = NULL;
  sfixed32_t is_exclusive = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t device_id = NULL;
  sfixed32_t carrier_id = NULL;
  sfixed32_t creative_audit_status = NULL;
  sfixed32_t is_creative_hosted = NULL;
  real64_t seller_deduction = NULL;
  sfixed32_t city = NULL;
  string_t latitude = NULL;
  string_t longitude = NULL;
  string_t device_unique_id = NULL;
  sfixed32_t package_id = NULL;
  string_t targeted_segments = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t is_toolbar = NULL;
  sfixed32_t deal_id = NULL;
  sfixed64_t vp_bitmap = NULL;
  sfixed32_t view_detection_enabled = NULL;
  enum_t view_result = NULL;
  sfixed32_t ozone_id = NULL;
  sfixed32_t is_performance = NULL;
  string_t sdk_version = NULL;
  sfixed32_t inventory_session_frequency = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t dma = NULL;
  string_t postal = NULL;
  sfixed32_t viewdef_definition_id = NULL;
  sfixed32_t viewdef_viewable = NULL;
  sfixed32_t view_measurable = NULL;
  sfixed32_t viewable = NULL;
  sfixed32_t is_secure = NULL;
  enum_t view_non_measurable_reason = NULL;
  data_cost[length_t] data_costs = NULL;
  sfixed32_t bidder_instance_id = NULL;
  sfixed32_t campaign_group_freq = NULL;
  sfixed32_t campaign_group_rec = NULL;
  sfixed32_t insertion_order_freq = NULL;
  sfixed32_t insertion_order_rec = NULL;
  string_t buyer_gender = NULL;
  sfixed32_t buyer_age = NULL;
  sfixed32_t[length_t] targeted_segment_list = NULL;
  sfixed32_t custom_model_id = NULL;
  fixed64_t custom_model_last_modified = NULL;
  string_t custom_model_output_code = NULL;
  string_t external_uid = NULL;
  string_t request_uuid = NULL;
  sfixed32_t mobile_app_instance_id = NULL;
  string_t traffic_source_code = NULL;
  string_t external_request_id = NULL;
  string_t stitch_group_id = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  sfixed32_t bid_priority = NULL;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  sfixed32_t explore_disposition = NULL;
  sfixed32_t device_make_id = NULL;
  sfixed32_t operating_system_family_id = NULL;
  tag_size[length_t] tag_sizes = NULL;
  campaign_group_model[length_t] campaign_group_models = NULL;
  sfixed32_t pricing_media_type = NULL;
  sfixed32_t buyer_trx_event_id = NULL;
  sfixed32_t seller_trx_event_id = NULL;
  sfixed32_t revenue_auction_event_type = NULL;
  bool_t is_prebid = NULL;
  bool_t is_unit_of_trx = NULL;
  sfixed32_t imps_for_budget_caps_pacing = NULL;
  fixed64_t auction_timestamp = NULL;
  bool_t two_phase_reduction_applied = NULL;
  sfixed32_t region_id = NULL;
  sfixed32_t media_company_id = NULL;
  sfixed32_t trade_agreement_id = NULL;
  personal_data personal_data = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  string_t gdpr_consent_cookie = NULL;
  sfixed32_t[length_t] additional_clearing_events = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  crossdevice_group_anonymized crossdevice_group_anon = NULL;
  crossdevice_graph_cost crossdevice_graph_cost = NULL;
  sfixed32_t revenue_event_type_id = NULL;
  sfixed32_t buyer_trx_event_type_id = NULL;
  sfixed32_t seller_trx_event_type_id = NULL;
  string_t external_creative_id = NULL;
  targeted_segment_details[length_t] targeted_segment_details = NULL;
  sfixed32_t bidder_seat_id = NULL;
  bool_t is_whiteops_scanned = NULL;
  string_t default_referrer_url = NULL;
  bool_t is_curated = NULL;
  sfixed32_t curator_member_id = NULL;
  fixed64_t total_partner_fees_microcents = NULL;
  real64_t net_buyer_spend = NULL;
  bool_t is_prebid_server = NULL;
  sfixed32_t cold_start_price_type = NULL;
  sfixed32_t discovery_state = NULL;
  fixed32_t billing_period_id = NULL;
  fixed32_t flight_id = NULL;
  sfixed32_t split_id = NULL;
  real64_t net_media_cost_dollars_cpm = NULL;
  fixed64_t total_data_costs_microcents = NULL;
  sfixed64_t total_profit_microcents = NULL;
  sfixed32_t targeted_crossdevice_graph_id = NULL;
  real64_t discovery_prediction = NULL;
  sfixed32_t campaign_group_type_id = NULL;
  sfixed32_t hb_source = NULL;
  string_t external_campaign_id = NULL;
  excluded_targeted_segment_details[length_t] excluded_targeted_segment_details = NULL;
  string_t trust_id = NULL;
  real64_t predicted_kpi_event_rate = NULL;
  bool_t has_crossdevice_reach_extension = NULL;
  crossdevice_group_anonymized[length_t] crossdevice_graph_membership = NULL;
  fixed64_t total_segment_data_costs_microcents = NULL;
  fixed64_t total_feature_costs_microcents = NULL;
  enum_t counterparty_ruleset_type = NULL;
  log_product_ads log_product_ads = NULL;
  string_t buyer_line_item_currency = NULL;
  string_t deal_line_item_currency = NULL;
  real64_t measurement_fee_usd = NULL;
  fixed32_t measurement_provider_member_id = NULL;
  fixed32_t offline_attribution_provider_member_id = NULL;
  real64_t offline_attribution_cost_usd_cpm = NULL;
  sfixed32_t pred_info = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  bool_t imp_rejecter_applied = NULL;
  real32_t ip_derived_latitude = NULL;
  real32_t ip_derived_longitude = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  sfixed32_t postal_code_ext_id = NULL;
  real64_t ecpm_conversion_rate = NULL;
  bool_t is_residential_ip = NULL;
  string_t hashed_ip = NULL;
  targeted_segment_details_by_id_type[length_t] targeted_segment_details_by_id_type = NULL;
  offline_attribution[length_t] offline_attribution = NULL;
  sfixed32_t frequency_cap_type_internal = NULL;
  bool_t modeled_cap_did_override_line_item_daily_cap = NULL;
  real64_t modeled_cap_user_sample_rate = NULL;
  real32_t estimated_audience_imps = NULL;
  real32_t audience_imps = NULL;
  fixed32_t[length_t] district_postal_code_lists = NULL;
  fixed32_t bidding_host_id = NULL;
  sfixed64_t buyer_dpvp_bitmap = NULL;
  sfixed64_t seller_dpvp_bitmap = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  fixed32_t feature_tests_bitmap = NULL;
  bool_t private_auction_eligible = NULL;
  enum_t chrome_traffic_label = NULL;
  bool_t is_private_auction = NULL;
  sfixed32_t creative_media_subtype_id = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
end;
constant
record
  string(int) name;
  record
    string(int) name;
    int number;
    unsigned integer(1) type_code;
    int message_type;
    unsigned integer(1) optional;
  end[int] field_infos;
end[int] agg_dw_impressions_message_types =
[vector
  [record
    name "data_cost"
    field_infos [vector
                  [record name "data_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost" number 2 type_code 1 message_type -1 optional 1],
                  [record name "used_segments" number 3 type_code 20 message_type -1 optional 1],
                  [record name "cost_pct" number 4 type_code 1 message_type -1 optional 1]]],
  [record
    name "pricing_term"
    field_infos [vector
                  [record name "term_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "amount" number 2 type_code 1 message_type -1 optional 1],
                  [record name "rate" number 3 type_code 1 message_type -1 optional 1],
                  [record name "is_deduction" number 4 type_code 7 message_type -1 optional 1],
                  [record name "is_media_cost_dependent" number 5 type_code 7 message_type -1 optional 1],
                  [record name "data_member_id" number 6 type_code 4 message_type -1 optional 1]]],
  [record
    name "member_pricing_term"
    field_infos [vector
                  [record name "rate_card_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "member_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "is_dw" number 3 type_code 7 message_type -1 optional 1],
                  [record name "pricing_terms" number 4 type_code 25 message_type 1 optional 1],
                  [record name "fx_margin_rate_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "marketplace_owner_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "virtual_marketplace_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "amino_enabled" number 8 type_code 7 message_type -1 optional 1]]],
  [record
    name "tag_size"
    field_infos [vector
                  [record name "width" number 1 type_code 4 message_type -1 optional 0],
                  [record name "height" number 2 type_code 4 message_type -1 optional 0]]],
  [record
    name "campaign_group_model"
    field_infos [vector
                  [record name "model_type" number 1 type_code 4 message_type -1 optional 1],
                  [record name "model_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "leaf_code" number 3 type_code 8 message_type -1 optional 1],
                  [record name "origin" number 4 type_code 4 message_type -1 optional 1],
                  [record name "experiment" number 5 type_code 4 message_type -1 optional 1],
                  [record name "value" number 6 type_code 2 message_type -1 optional 1]]],
  [record
    name "crossdevice_group"
    field_infos [vector
                  [record name "graph_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "group_id" number 2 type_code 3 message_type -1 optional 1]]],
  [record
    name "personal_data"
    field_infos [vector
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "device_unique_id" number 4 type_code 8 message_type -1 optional 1],
                  [record name "external_uid" number 5 type_code 8 message_type -1 optional 1],
                  [record name "ip_address" number 7 type_code 8 message_type -1 optional 1],
                  [record name "crossdevice_group" number 8 type_code 9 message_type 5 optional 1],
                  [record name "latitude" number 9 type_code 1 message_type -1 optional 1],
                  [record name "longitude" number 10 type_code 1 message_type -1 optional 1],
                  [record name "ipv6_address" number 11 type_code 8 message_type -1 optional 1],
                  [record name "subject_to_gdpr" number 12 type_code 7 message_type -1 optional 1],
                  [record name "geo_country" number 13 type_code 8 message_type -1 optional 1],
                  [record name "gdpr_consent_string" number 14 type_code 8 message_type -1 optional 1],
                  [record name "preempt_ip_address" number 15 type_code 8 message_type -1 optional 1],
                  [record name "device_type" number 16 type_code 4 message_type -1 optional 1],
                  [record name "device_make_id" number 17 type_code 4 message_type -1 optional 1],
                  [record name "device_model_id" number 18 type_code 4 message_type -1 optional 1],
                  [record name "new_user_id_64" number 19 type_code 1 message_type -1 optional 1],
                  [record name "is_service_provider_mode" number 20 type_code 7 message_type -1 optional 1],
                  [record name "is_personal_info_sale" number 21 type_code 7 message_type -1 optional 1]]],
  [record
    name "log_anonymized_user_info"
    field_infos [vector
                  [record name "user_id" number 1 type_code 8 message_type -1 optional 1]]],
  [record
    name "crossdevice_group_anonymized"
    field_infos [vector
                  [record name "graph_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "group_id" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "crossdevice_graph_cost"
    field_infos [vector
                  [record name "graph_provider_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost_cpm_usd" number 2 type_code 1 message_type -1 optional 1]]],
  [record
    name "targeted_segment_details"
    field_infos [vector
                  [record name "segment_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "last_seen_min" number 2 type_code 4 message_type -1 optional 1]]],
  [record
    name "excluded_targeted_segment_details"
    field_infos [vector
                  [record name "segment_id" number 1 type_code 4 message_type -1 optional 1]]],
  [record
    name "log_product_ads"
    field_infos [vector
                  [record name "product_feed_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "item_selection_strategy_id" number 2 type_code 4 message_type -1 optional 1],
                  [record name "product_uuid" number 3 type_code 8 message_type -1 optional 1]]],
  [record
    name "log_personal_identifier"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 0],
                  [record name "identity_value" number 2 type_code 8 message_type -1 optional 1]]],
  [record
    name "targeted_segment_details_by_id_type"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segment_details" number 2 type_code 25 message_type 10 optional 1]]],
  [record
    name "offline_attribution"
    field_infos [vector
                  [record name "provider_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost_usd_cpm" number 2 type_code 1 message_type -1 optional 1]]],
  [record
    name "agg_dw_impressions"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "user_id_64" number 3 type_code 1 message_type -1 optional 1],
                  [record name "tag_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "venue_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "inventory_source_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "session_frequency" number 7 type_code 4 message_type -1 optional 1],
                  [record name "site_domain" number 8 type_code 8 message_type -1 optional 1],
                  [record name "width" number 9 type_code 4 message_type -1 optional 1],
                  [record name "height" number 10 type_code 4 message_type -1 optional 1],
                  [record name "geo_country" number 11 type_code 8 message_type -1 optional 1],
                  [record name "geo_region" number 12 type_code 8 message_type -1 optional 1],
                  [record name "gender" number 13 type_code 8 message_type -1 optional 1],
                  [record name "age" number 14 type_code 4 message_type -1 optional 1],
                  [record name "seller_member_id" number 15 type_code 4 message_type -1 optional 1],
                  [record name "buyer_member_id" number 16 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 17 type_code 4 message_type -1 optional 1],
                  [record name "seller_currency" number 18 type_code 8 message_type -1 optional 1],
                  [record name "buyer_currency" number 19 type_code 8 message_type -1 optional 1],
                  [record name "buyer_bid" number 20 type_code 1 message_type -1 optional 1],
                  [record name "buyer_spend" number 21 type_code 1 message_type -1 optional 1],
                  [record name "ecp" number 22 type_code 1 message_type -1 optional 1],
                  [record name "reserve_price" number 23 type_code 1 message_type -1 optional 1],
                  [record name "advertiser_id" number 24 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_id" number 25 type_code 4 message_type -1 optional 1],
                  [record name "campaign_id" number 26 type_code 4 message_type -1 optional 1],
                  [record name "creative_freq" number 27 type_code 4 message_type -1 optional 1],
                  [record name "creative_rec" number 28 type_code 4 message_type -1 optional 1],
                  [record name "is_learn" number 29 type_code 4 message_type -1 optional 1],
                  [record name "is_remarketing" number 30 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_frequency" number 31 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_recency" number 32 type_code 4 message_type -1 optional 1],
                  [record name "user_group_id" number 33 type_code 4 message_type -1 optional 1],
                  [record name "camp_dp_id" number 34 type_code 4 message_type -1 optional 1],
                  [record name "media_buy_id" number 35 type_code 4 message_type -1 optional 1],
                  [record name "media_buy_cost" number 36 type_code 1 message_type -1 optional 1],
                  [record name "brand_id" number 37 type_code 4 message_type -1 optional 1],
                  [record name "cleared_direct" number 38 type_code 4 message_type -1 optional 1],
                  [record name "clear_fees" number 39 type_code 1 message_type -1 optional 1],
                  [record name "media_buy_rev_share_pct" number 40 type_code 1 message_type -1 optional 1],
                  [record name "revenue_value" number 41 type_code 1 message_type -1 optional 1],
                  [record name "pricing_type" number 42 type_code 8 message_type -1 optional 1],
                  [record name "can_convert" number 43 type_code 4 message_type -1 optional 1],
                  [record name "pub_rule_id" number 44 type_code 4 message_type -1 optional 1],
                  [record name "is_control" number 45 type_code 4 message_type -1 optional 1],
                  [record name "control_pct" number 46 type_code 1 message_type -1 optional 1],
                  [record name "control_creative_id" number 47 type_code 4 message_type -1 optional 1],
                  [record name "predicted_cpm" number 48 type_code 1 message_type -1 optional 1],
                  [record name "actual_bid" number 49 type_code 1 message_type -1 optional 1],
                  [record name "site_id" number 50 type_code 4 message_type -1 optional 1],
                  [record name "content_category_id" number 51 type_code 4 message_type -1 optional 1],
                  [record name "auction_service_fees" number 52 type_code 1 message_type -1 optional 1],
                  [record name "discrepancy_allowance" number 53 type_code 1 message_type -1 optional 1],
                  [record name "forex_allowance" number 54 type_code 1 message_type -1 optional 1],
                  [record name "creative_overage_fees" number 55 type_code 1 message_type -1 optional 1],
                  [record name "fold_position" number 56 type_code 4 message_type -1 optional 1],
                  [record name "external_inv_id" number 57 type_code 4 message_type -1 optional 1],
                  [record name "cadence_modifier" number 58 type_code 1 message_type -1 optional 1],
                  [record name "imp_type" number 59 type_code 4 message_type -1 optional 0],
                  [record name "advertiser_currency" number 60 type_code 8 message_type -1 optional 1],
                  [record name "advertiser_exchange_rate" number 61 type_code 1 message_type -1 optional 1],
                  [record name "ip_address" number 62 type_code 8 message_type -1 optional 1],
                  [record name "publisher_id" number 63 type_code 4 message_type -1 optional 1],
                  [record name "auction_service_deduction" number 64 type_code 1 message_type -1 optional 1],
                  [record name "insertion_order_id" number 65 type_code 4 message_type -1 optional 1],
                  [record name "predict_type_rev" number 66 type_code 4 message_type -1 optional 1],
                  [record name "predict_type_goal" number 67 type_code 4 message_type -1 optional 1],
                  [record name "predict_type_cost" number 68 type_code 4 message_type -1 optional 1],
                  [record name "booked_revenue_dollars" number 69 type_code 1 message_type -1 optional 1],
                  [record name "booked_revenue_adv_curr" number 70 type_code 1 message_type -1 optional 1],
                  [record name "commission_cpm" number 71 type_code 1 message_type -1 optional 1],
                  [record name "commission_revshare" number 72 type_code 1 message_type -1 optional 1],
                  [record name "serving_fees_cpm" number 73 type_code 1 message_type -1 optional 1],
                  [record name "serving_fees_revshare" number 74 type_code 1 message_type -1 optional 1],
                  [record name "user_tz_offset" number 75 type_code 4 message_type -1 optional 1],
                  [record name "media_type" number 76 type_code 4 message_type -1 optional 1],
                  [record name "operating_system" number 77 type_code 4 message_type -1 optional 1],
                  [record name "browser" number 78 type_code 4 message_type -1 optional 1],
                  [record name "language" number 79 type_code 4 message_type -1 optional 1],
                  [record name "publisher_currency" number 80 type_code 8 message_type -1 optional 1],
                  [record name "publisher_exchange_rate" number 81 type_code 1 message_type -1 optional 1],
                  [record name "media_cost_dollars_cpm" number 82 type_code 1 message_type -1 optional 1],
                  [record name "payment_type" number 83 type_code 4 message_type -1 optional 1],
                  [record name "revenue_type" number 84 type_code 4 message_type -1 optional 1],
                  [record name "seller_revenue_cpm" number 85 type_code 1 message_type -1 optional 1],
                  [record name "bidder_id" number 86 type_code 4 message_type -1 optional 1],
                  [record name "inv_code" number 87 type_code 8 message_type -1 optional 1],
                  [record name "application_id" number 88 type_code 8 message_type -1 optional 1],
                  [record name "shadow_price" number 89 type_code 1 message_type -1 optional 1],
                  [record name "eap" number 90 type_code 1 message_type -1 optional 1],
                  [record name "datacenter_id" number 91 type_code 4 message_type -1 optional 1],
                  [record name "imp_blacklist_or_fraud" number 92 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_domains" number 94 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_categories" number 95 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_pubs" number 96 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_tag" number 97 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_age" number 98 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_gender" number 99 type_code 4 message_type -1 optional 1],
                  [record name "inventory_url_id" number 100 type_code 4 message_type -1 optional 1],
                  [record name "audit_type" number 101 type_code 4 message_type -1 optional 1],
                  [record name "is_exclusive" number 102 type_code 4 message_type -1 optional 1],
                  [record name "truncate_ip" number 103 type_code 4 message_type -1 optional 1],
                  [record name "device_id" number 104 type_code 4 message_type -1 optional 1],
                  [record name "carrier_id" number 105 type_code 4 message_type -1 optional 1],
                  [record name "creative_audit_status" number 106 type_code 4 message_type -1 optional 1],
                  [record name "is_creative_hosted" number 107 type_code 4 message_type -1 optional 1],
                  [record name "seller_deduction" number 108 type_code 1 message_type -1 optional 1],
                  [record name "city" number 109 type_code 4 message_type -1 optional 1],
                  [record name "latitude" number 110 type_code 8 message_type -1 optional 1],
                  [record name "longitude" number 111 type_code 8 message_type -1 optional 1],
                  [record name "device_unique_id" number 112 type_code 8 message_type -1 optional 1],
                  [record name "package_id" number 113 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segments" number 114 type_code 8 message_type -1 optional 1],
                  [record name "supply_type" number 115 type_code 4 message_type -1 optional 1],
                  [record name "is_toolbar" number 116 type_code 4 message_type -1 optional 1],
                  [record name "deal_id" number 117 type_code 4 message_type -1 optional 1],
                  [record name "vp_bitmap" number 118 type_code 3 message_type -1 optional 1],
                  [record name "view_detection_enabled" number 119 type_code 4 message_type -1 optional 1],
                  [record name "view_result" number 120 type_code 4 message_type -1 optional 1],
                  [record name "ozone_id" number 121 type_code 4 message_type -1 optional 1],
                  [record name "is_performance" number 122 type_code 4 message_type -1 optional 1],
                  [record name "sdk_version" number 123 type_code 8 message_type -1 optional 1],
                  [record name "inventory_session_frequency" number 124 type_code 4 message_type -1 optional 1],
                  [record name "device_type" number 125 type_code 4 message_type -1 optional 1],
                  [record name "dma" number 126 type_code 4 message_type -1 optional 1],
                  [record name "postal" number 127 type_code 8 message_type -1 optional 1],
                  [record name "viewdef_definition_id" number 128 type_code 4 message_type -1 optional 1],
                  [record name "viewdef_viewable" number 129 type_code 4 message_type -1 optional 1],
                  [record name "view_measurable" number 130 type_code 4 message_type -1 optional 1],
                  [record name "viewable" number 131 type_code 4 message_type -1 optional 1],
                  [record name "is_secure" number 132 type_code 4 message_type -1 optional 1],
                  [record name "view_non_measurable_reason" number 133 type_code 4 message_type -1 optional 1],
                  [record name "data_costs" number 134 type_code 25 message_type 0 optional 1],
                  [record name "bidder_instance_id" number 135 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_freq" number 136 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_rec" number 137 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_freq" number 138 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_rec" number 139 type_code 4 message_type -1 optional 1],
                  [record name "buyer_gender" number 140 type_code 8 message_type -1 optional 1],
                  [record name "buyer_age" number 141 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segment_list" number 142 type_code 20 message_type -1 optional 1],
                  [record name "custom_model_id" number 143 type_code 4 message_type -1 optional 1],
                  [record name "custom_model_last_modified" number 144 type_code 1 message_type -1 optional 1],
                  [record name "custom_model_output_code" number 145 type_code 8 message_type -1 optional 1],
                  [record name "external_uid" number 146 type_code 8 message_type -1 optional 1],
                  [record name "request_uuid" number 147 type_code 8 message_type -1 optional 1],
                  [record name "mobile_app_instance_id" number 148 type_code 4 message_type -1 optional 1],
                  [record name "traffic_source_code" number 149 type_code 8 message_type -1 optional 1],
                  [record name "external_request_id" number 150 type_code 8 message_type -1 optional 1],
                  [record name "stitch_group_id" number 151 type_code 8 message_type -1 optional 1],
                  [record name "deal_type" number 152 type_code 4 message_type -1 optional 1],
                  [record name "ym_floor_id" number 153 type_code 4 message_type -1 optional 1],
                  [record name "ym_bias_id" number 154 type_code 4 message_type -1 optional 1],
                  [record name "bid_priority" number 155 type_code 4 message_type -1 optional 1],
                  [record name "buyer_charges" number 156 type_code 9 message_type 2 optional 1],
                  [record name "seller_charges" number 157 type_code 9 message_type 2 optional 1],
                  [record name "explore_disposition" number 158 type_code 4 message_type -1 optional 1],
                  [record name "device_make_id" number 159 type_code 4 message_type -1 optional 1],
                  [record name "operating_system_family_id" number 160 type_code 4 message_type -1 optional 1],
                  [record name "tag_sizes" number 161 type_code 25 message_type 3 optional 1],
                  [record name "campaign_group_models" number 162 type_code 25 message_type 4 optional 1],
                  [record name "pricing_media_type" number 163 type_code 4 message_type -1 optional 1],
                  [record name "buyer_trx_event_id" number 164 type_code 4 message_type -1 optional 1],
                  [record name "seller_trx_event_id" number 165 type_code 4 message_type -1 optional 1],
                  [record name "revenue_auction_event_type" number 166 type_code 4 message_type -1 optional 1],
                  [record name "is_prebid" number 167 type_code 7 message_type -1 optional 1],
                  [record name "is_unit_of_trx" number 168 type_code 7 message_type -1 optional 1],
                  [record name "imps_for_budget_caps_pacing" number 169 type_code 4 message_type -1 optional 1],
                  [record name "auction_timestamp" number 170 type_code 1 message_type -1 optional 1],
                  [record name "two_phase_reduction_applied" number 173 type_code 7 message_type -1 optional 1],
                  [record name "region_id" number 174 type_code 4 message_type -1 optional 1],
                  [record name "media_company_id" number 175 type_code 4 message_type -1 optional 1],
                  [record name "trade_agreement_id" number 176 type_code 4 message_type -1 optional 1],
                  [record name "personal_data" number 177 type_code 9 message_type 6 optional 1],
                  [record name "anonymized_user_info" number 178 type_code 9 message_type 7 optional 1],
                  [record name "gdpr_consent_cookie" number 179 type_code 8 message_type -1 optional 1],
                  [record name "additional_clearing_events" number 180 type_code 20 message_type -1 optional 1],
                  [record name "fx_rate_snapshot_id" number 181 type_code 4 message_type -1 optional 1],
                  [record name "crossdevice_group_anon" number 182 type_code 9 message_type 8 optional 1],
                  [record name "crossdevice_graph_cost" number 183 type_code 9 message_type 9 optional 1],
                  [record name "revenue_event_type_id" number 184 type_code 4 message_type -1 optional 1],
                  [record name "buyer_trx_event_type_id" number 185 type_code 4 message_type -1 optional 1],
                  [record name "seller_trx_event_type_id" number 186 type_code 4 message_type -1 optional 1],
                  [record name "external_creative_id" number 187 type_code 8 message_type -1 optional 1],
                  [record name "targeted_segment_details" number 188 type_code 25 message_type 10 optional 1],
                  [record name "bidder_seat_id" number 189 type_code 4 message_type -1 optional 1],
                  [record name "is_whiteops_scanned" number 190 type_code 7 message_type -1 optional 1],
                  [record name "default_referrer_url" number 191 type_code 8 message_type -1 optional 1],
                  [record name "is_curated" number 192 type_code 7 message_type -1 optional 1],
                  [record name "curator_member_id" number 193 type_code 4 message_type -1 optional 1],
                  [record name "total_partner_fees_microcents" number 194 type_code 3 message_type -1 optional 1],
                  [record name "net_buyer_spend" number 195 type_code 1 message_type -1 optional 1],
                  [record name "is_prebid_server" number 196 type_code 7 message_type -1 optional 1],
                  [record name "cold_start_price_type" number 197 type_code 4 message_type -1 optional 1],
                  [record name "discovery_state" number 198 type_code 4 message_type -1 optional 1],
                  [record name "billing_period_id" number 199 type_code 4 message_type -1 optional 1],
                  [record name "flight_id" number 200 type_code 4 message_type -1 optional 1],
                  [record name "split_id" number 201 type_code 4 message_type -1 optional 1],
                  [record name "net_media_cost_dollars_cpm" number 202 type_code 1 message_type -1 optional 1],
                  [record name "total_data_costs_microcents" number 203 type_code 3 message_type -1 optional 1],
                  [record name "total_profit_microcents" number 204 type_code 3 message_type -1 optional 1],
                  [record name "targeted_crossdevice_graph_id" number 205 type_code 4 message_type -1 optional 1],
                  [record name "discovery_prediction" number 206 type_code 1 message_type -1 optional 1],
                  [record name "campaign_group_type_id" number 207 type_code 4 message_type -1 optional 1],
                  [record name "hb_source" number 208 type_code 4 message_type -1 optional 1],
                  [record name "external_campaign_id" number 209 type_code 8 message_type -1 optional 1],
                  [record name "excluded_targeted_segment_details" number 210 type_code 25 message_type 11 optional 1],
                  [record name "trust_id" number 211 type_code 8 message_type -1 optional 1],
                  [record name "predicted_kpi_event_rate" number 212 type_code 1 message_type -1 optional 1],
                  [record name "has_crossdevice_reach_extension" number 213 type_code 7 message_type -1 optional 1],
                  [record name "crossdevice_graph_membership" number 214 type_code 25 message_type 8 optional 1],
                  [record name "total_segment_data_costs_microcents" number 215 type_code 3 message_type -1 optional 1],
                  [record name "total_feature_costs_microcents" number 216 type_code 3 message_type -1 optional 1],
                  [record name "counterparty_ruleset_type" number 217 type_code 4 message_type -1 optional 1],
                  [record name "log_product_ads" number 218 type_code 9 message_type 12 optional 1],
                  [record name "buyer_line_item_currency" number 219 type_code 8 message_type -1 optional 1],
                  [record name "deal_line_item_currency" number 220 type_code 8 message_type -1 optional 1],
                  [record name "measurement_fee_usd" number 221 type_code 1 message_type -1 optional 1],
                  [record name "measurement_provider_member_id" number 222 type_code 4 message_type -1 optional 1],
                  [record name "offline_attribution_provider_member_id" number 223 type_code 4 message_type -1 optional 1],
                  [record name "offline_attribution_cost_usd_cpm" number 225 type_code 1 message_type -1 optional 1],
                  [record name "pred_info" number 226 type_code 4 message_type -1 optional 1],
                  [record name "imp_rejecter_do_auction" number 227 type_code 7 message_type -1 optional 1],
                  [record name "personal_identifiers" number 228 type_code 25 message_type 13 optional 1],
                  [record name "imp_rejecter_applied" number 229 type_code 7 message_type -1 optional 1],
                  [record name "ip_derived_latitude" number 230 type_code 2 message_type -1 optional 1],
                  [record name "ip_derived_longitude" number 231 type_code 2 message_type -1 optional 1],
                  [record name "personal_identifiers_experimental" number 232 type_code 25 message_type 13 optional 1],
                  [record name "postal_code_ext_id" number 233 type_code 4 message_type -1 optional 1],
                  [record name "ecpm_conversion_rate" number 234 type_code 1 message_type -1 optional 1],
                  [record name "is_residential_ip" number 235 type_code 7 message_type -1 optional 1],
                  [record name "hashed_ip" number 236 type_code 8 message_type -1 optional 1],
                  [record name "targeted_segment_details_by_id_type" number 237 type_code 25 message_type 14 optional 1],
                  [record name "offline_attribution" number 238 type_code 25 message_type 15 optional 1],
                  [record name "frequency_cap_type_internal" number 239 type_code 4 message_type -1 optional 1],
                  [record name "modeled_cap_did_override_line_item_daily_cap" number 240 type_code 7 message_type -1 optional 1],
                  [record name "modeled_cap_user_sample_rate" number 241 type_code 1 message_type -1 optional 1],
                  [record name "estimated_audience_imps" number 242 type_code 2 message_type -1 optional 1],
                  [record name "audience_imps" number 243 type_code 2 message_type -1 optional 1],
                  [record name "district_postal_code_lists" number 244 type_code 20 message_type -1 optional 1],
                  [record name "bidding_host_id" number 245 type_code 4 message_type -1 optional 1],
                  [record name "buyer_dpvp_bitmap" number 246 type_code 3 message_type -1 optional 1],
                  [record name "seller_dpvp_bitmap" number 247 type_code 3 message_type -1 optional 1],
                  [record name "browser_code_id" number 248 type_code 4 message_type -1 optional 1],
                  [record name "is_prebid_server_included" number 249 type_code 4 message_type -1 optional 1],
                  [record name "feature_tests_bitmap" number 250 type_code 4 message_type -1 optional 1],
                  [record name "private_auction_eligible" number 251 type_code 7 message_type -1 optional 1],
                  [record name "chrome_traffic_label" number 252 type_code 4 message_type -1 optional 1],
                  [record name "is_private_auction" number 253 type_code 7 message_type -1 optional 1],
                  [record name "creative_media_subtype_id" number 254 type_code 4 message_type -1 optional 1],
                  [record name "allowed_media_types" number 255 type_code 20 message_type -1 optional 1]]]];
metadata type = agg_dw_impressions ;"""
      ).map(s => parse(s).asInstanceOf[FFSchemaRecord])
      var writer = in.write.format("io.prophecy.libs.FixedFileFormat")
      writer = writer.mode("overwrite")
      schema
        .map(s => Json.stringify(Json.toJson(s)))
        .foreach(schema => writer = writer.option("schema", schema))
      writer.save("NA")
    } catch {
      case e: Error =>
        println(s"Error occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while writing dataframe: $e")
        throw new Exception(e.getMessage)
    }
  }

}
