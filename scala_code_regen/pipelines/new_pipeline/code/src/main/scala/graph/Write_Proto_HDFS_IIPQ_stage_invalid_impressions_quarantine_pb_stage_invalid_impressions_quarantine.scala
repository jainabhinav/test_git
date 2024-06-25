package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_IIPQ_stage_invalid_impressions_quarantine_pb_stage_invalid_impressions_quarantine {

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
type crossdevice_graph_cost =
record
  sfixed32_t graph_provider_member_id = NULL;
  real64_t cost_cpm_usd = NULL;
end;
type crossdevice_group =
record
  sfixed32_t graph_id = NULL;
  sfixed64_t group_id = NULL;
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
type revenue_info =
record
  fixed64_t total_partner_fees_microcents = NULL;
  real64_t booked_revenue_dollars = NULL;
  real64_t booked_revenue_adv_curr = NULL;
  fixed64_t total_data_costs_microcents = NULL;
  sfixed64_t total_profit_microcents = NULL;
  fixed64_t total_segment_data_costs_microcents = NULL;
  fixed64_t total_feature_costs_microcents = NULL;
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
type valuation_landscape =
record
  sfixed32_t kpi_event_id = NULL;
  real64_t ev_kpi_event_ac = NULL;
  real64_t p_kpi_event = NULL;
  real64_t bpo_aggressiveness_factor = NULL;
  real64_t min_margin_pct = NULL;
  real64_t max_revenue_or_bid_value = NULL;
  real64_t min_revenue_or_bid_value = NULL;
  real64_t cold_start_price_ac = NULL;
  real64_t dynamic_bid_max_revenue_ac = NULL;
  real64_t p_revenue_event = NULL;
  real64_t total_fees_deducted_ac = NULL;
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
type log_dw_bid =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  real64_t price = NULL;
  sfixed32_t member_id = NULL;
  sfixed32_t advertiser_id = NULL;
  sfixed32_t campaign_group_id = NULL;
  sfixed32_t campaign_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t creative_freq = NULL;
  sfixed32_t creative_rec = NULL;
  sfixed32_t advertiser_freq = NULL;
  sfixed32_t advertiser_rec = NULL;
  sfixed32_t is_remarketing = NULL;
  sfixed32_t user_group_id = NULL;
  real64_t media_buy_cost = NULL;
  sfixed32_t is_default = NULL;
  sfixed32_t pub_rule_id = NULL;
  real64_t media_buy_rev_share_pct = NULL;
  string_t pricing_type = NULL;
  sfixed32_t can_convert = NULL;
  sfixed32_t is_control = NULL;
  real64_t control_pct = NULL;
  sfixed32_t control_creative_id = NULL;
  real64_t cadence_modifier = NULL;
  string_t advertiser_currency = NULL;
  real64_t advertiser_exchange_rate = NULL;
  sfixed32_t insertion_order_id = NULL;
  sfixed32_t predict_type = NULL;
  sfixed32_t predict_type_goal = NULL;
  real64_t revenue_value_dollars = NULL;
  real64_t revenue_value_adv_curr = NULL;
  real64_t commission_cpm = NULL;
  real64_t commission_revshare = NULL;
  real64_t serving_fees_cpm = NULL;
  real64_t serving_fees_revshare = NULL;
  string_t publisher_currency = NULL;
  real64_t publisher_exchange_rate = NULL;
  enum_t payment_type = NULL;
  real64_t payment_value = NULL;
  sfixed32_t creative_group_freq = NULL;
  sfixed32_t creative_group_rec = NULL;
  enum_t revenue_type = NULL;
  sfixed32_t apply_cost_on_default = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t vp_expose_age = NULL;
  sfixed32_t vp_expose_gender = NULL;
  string_t targeted_segments = NULL;
  sfixed32_t ttl = NULL;
  fixed64_t auction_timestamp;
  data_cost[length_t] data_costs = NULL;
  sfixed32_t[length_t] targeted_segment_list = NULL;
  sfixed32_t campaign_group_freq = NULL;
  sfixed32_t campaign_group_rec = NULL;
  sfixed32_t insertion_order_freq = NULL;
  sfixed32_t insertion_order_rec = NULL;
  string_t buyer_gender = NULL;
  sfixed32_t buyer_age = NULL;
  sfixed32_t custom_model_id = NULL;
  fixed64_t custom_model_last_modified = NULL;
  string_t custom_model_output_code = NULL;
  sfixed32_t bid_priority = NULL;
  sfixed32_t explore_disposition = NULL;
  sfixed32_t revenue_auction_event_type = NULL;
  campaign_group_model[length_t] campaign_group_models = NULL;
  enum_t impression_transaction_type = NULL;
  sfixed32_t is_deferred = NULL;
  enum_t log_type = NULL;
  crossdevice_group_anonymized crossdevice_group_anon = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  crossdevice_graph_cost crossdevice_graph_cost = NULL;
  sfixed32_t revenue_event_type_id = NULL;
  targeted_segment_details[length_t] targeted_segment_details = NULL;
  fixed32_t insertion_order_budget_interval_id = NULL;
  fixed32_t campaign_group_budget_interval_id = NULL;
  sfixed32_t cold_start_price_type = NULL;
  sfixed32_t discovery_state = NULL;
  revenue_info revenue_info = NULL;
  bool_t use_revenue_info = NULL;
  real64_t sales_tax_rate_pct = NULL;
  sfixed32_t targeted_crossdevice_graph_id = NULL;
  fixed32_t product_feed_id = NULL;
  fixed32_t item_selection_strategy_id = NULL;
  real64_t discovery_prediction = NULL;
  fixed32_t bidding_host_id = NULL;
  fixed32_t split_id = NULL;
  excluded_targeted_segment_details[length_t] excluded_targeted_segment_details = NULL;
  real64_t predicted_kpi_event_rate = NULL;
  bool_t has_crossdevice_reach_extension = NULL;
  real64_t advertiser_expected_value_ecpm_ac = NULL;
  real64_t bpp_multiplier = NULL;
  real64_t bpp_offset = NULL;
  real64_t bid_modifier = NULL;
  fixed64_t payment_value_microcents = NULL;
  crossdevice_group_anonymized[length_t] crossdevice_graph_membership = NULL;
  valuation_landscape[length_t] valuation_landscape = NULL;
  string_t line_item_currency = NULL;
  real64_t measurement_fee_cpm_usd = NULL;
  fixed32_t measurement_provider_id = NULL;
  fixed32_t measurement_provider_member_id = NULL;
  fixed32_t offline_attribution_provider_member_id = NULL;
  real64_t offline_attribution_cost_usd_cpm = NULL;
  targeted_segment_details_by_id_type[length_t] targeted_segment_details_by_id_type = NULL;
  offline_attribution[length_t] offline_attribution = NULL;
  sfixed32_t frequency_cap_type_internal = NULL;
  bool_t modeled_cap_did_override_line_item_daily_cap = NULL;
  real64_t modeled_cap_user_sample_rate = NULL;
  real64_t bid_rate = NULL;
  fixed32_t[length_t] district_postal_code_lists = NULL;
  real64_t pre_bpp_price = NULL;
  fixed32_t feature_tests_bitmap = NULL;
end;
type log_dw_view =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  string_t advertiser_currency = NULL;
  real64_t advertiser_exchange_rate = NULL;
  real64_t booked_revenue_dollars = NULL;
  real64_t booked_revenue_adv_curr = NULL;
  string_t publisher_currency = NULL;
  real64_t publisher_exchange_rate = NULL;
  real64_t payment_value = NULL;
  string_t ip_address = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t view_auction_event_type = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  sfixed32_t view_event_type_id = NULL;
  revenue_info revenue_info = NULL;
  bool_t use_revenue_info = NULL;
  bool_t is_deferred = NULL;
  real64_t ecpm_conversion_rate = NULL;
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
type transaction_event_pricing =
record
  fixed64_t gross_payment_value_microcents = NULL;
  fixed64_t net_payment_value_microcents = NULL;
  fixed64_t seller_revenue_microcents = NULL;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  bool_t buyer_transacted = NULL;
  bool_t seller_transacted = NULL;
end;
type log_impbus_auction_event =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t payment_value_microcents = NULL;
  sfixed32_t transaction_event = NULL;
  sfixed32_t transaction_event_type_id = NULL;
  bool_t is_deferred = NULL;
  transaction_event_pricing auction_event_pricing = NULL;
end;
type tag_size =
record
  sfixed32_t width;
  sfixed32_t height;
end;
type log_product_ads =
record
  sfixed32_t product_feed_id = NULL;
  sfixed32_t item_selection_strategy_id = NULL;
  string_t product_uuid = NULL;
end;
type log_transaction_def =
record
  sfixed32_t transaction_event = NULL;
  sfixed32_t transaction_event_type_id = NULL;
end;
type log_predicted_video_view_info =
record
  real64_t iab_view_rate_over_measured = NULL;
  real64_t iab_view_rate_over_total = NULL;
  real64_t predicted_100pv50pd_video_view_rate = NULL;
  real64_t predicted_100pv50pd_video_view_rate_over_total = NULL;
  real64_t video_completion_rate = NULL;
  sfixed32_t view_prediction_source = NULL;
end;
type log_auction_url_info =
record
  string_t site_url = NULL;
end;
type log_location_info =
record
  real32_t latitude = NULL;
  real32_t longitude = NULL;
end;
type log_engagement_rate =
record
  sfixed32_t engagement_rate_type = NULL;
  real64_t rate = NULL;
  sfixed32_t engagement_rate_type_id = NULL;
end;
type log_personal_identifier =
record
  fixed32_t identity_type;
  string_t identity_value = NULL;
end;
type log_impbus_impressions =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tag_id = NULL;
  string_t ip_address = NULL;
  sfixed32_t venue_id = NULL;
  string_t site_domain = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  string_t geo_country = NULL;
  string_t geo_region = NULL;
  string_t gender = NULL;
  sfixed32_t age = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  sfixed32_t imp_bid_on = NULL;
  real64_t buyer_bid = NULL;
  real64_t buyer_spend = NULL;
  real64_t seller_revenue = NULL;
  sfixed32_t num_of_bids = NULL;
  real64_t ecp = NULL;
  real64_t reserve_price = NULL;
  string_t inv_code = NULL;
  string_t call_type = NULL;
  sfixed32_t inventory_source_id = NULL;
  sfixed32_t cookie_age = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t cleared_direct = NULL;
  real64_t forex_allowance = NULL;
  sfixed32_t fold_position = NULL;
  sfixed32_t external_inv_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_delivered = NULL;
  sfixed32_t is_dw = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t content_category_id = NULL;
  sfixed32_t datacenter_id = NULL;
  real64_t eap = NULL;
  sfixed32_t user_tz_offset = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t language = NULL;
  string_t application_id = NULL;
  string_t user_locale = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t audit_type = NULL;
  real64_t shadow_price = NULL;
  sfixed32_t impbus_id = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_exchange_rate = NULL;
  string_t seller_currency = NULL;
  real64_t seller_exchange_rate = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t vp_expose_categories = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  sfixed32_t vp_expose_tag = NULL;
  sfixed32_t is_exclusive = NULL;
  sfixed32_t bidder_instance_id = NULL;
  sfixed32_t visibility_profile_id = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t device_id = NULL;
  sfixed32_t carrier_id = NULL;
  sfixed32_t creative_audit_status = NULL;
  sfixed32_t is_creative_hosted = NULL;
  sfixed32_t city = NULL;
  string_t latitude = NULL;
  string_t longitude = NULL;
  string_t device_unique_id = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t is_toolbar = NULL;
  sfixed32_t deal_id = NULL;
  sfixed64_t vp_bitmap = NULL;
  sfixed32_t ttl = NULL;
  sfixed32_t view_detection_enabled = NULL;
  sfixed32_t ozone_id = NULL;
  sfixed32_t is_performance = NULL;
  string_t sdk_version = NULL;
  sfixed32_t inventory_session_frequency = NULL;
  sfixed32_t bid_price_type = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t dma = NULL;
  string_t postal = NULL;
  sfixed32_t package_id = NULL;
  sfixed32_t spend_protection = NULL;
  sfixed32_t is_secure = NULL;
  real64_t estimated_view_rate = NULL;
  string_t external_request_id = NULL;
  sfixed32_t viewdef_definition_id_buyer_member = NULL;
  sfixed32_t spend_protection_pixel_id = NULL;
  string_t external_uid = NULL;
  string_t request_uuid = NULL;
  sfixed32_t mobile_app_instance_id = NULL;
  string_t traffic_source_code = NULL;
  string_t stitch_group_id = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  real64_t estimated_view_rate_over_total = NULL;
  sfixed32_t device_make_id = NULL;
  sfixed32_t operating_system_family_id = NULL;
  tag_size[length_t] tag_sizes = NULL;
  log_transaction_def seller_transaction_def = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  log_predicted_video_view_info predicted_video_view_info = NULL;
  log_auction_url_info auction_url = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t is_imp_rejecter_applied = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  log_location_info geo_location = NULL;
  real64_t seller_bid_currency_conversion_rate = NULL;
  string_t seller_bid_currency_code = NULL;
  bool_t is_prebid = NULL;
  string_t default_referrer_url = NULL;
  log_engagement_rate[length_t] engagement_rates = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  enum_t payment_type = NULL;
  sfixed32_t apply_cost_on_default = NULL;
  real64_t media_buy_cost = NULL;
  real64_t media_buy_rev_share_pct = NULL;
  sfixed32_t auction_duration_ms = NULL;
  sfixed32_t expected_events = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  sfixed32_t region_id = NULL;
  sfixed32_t media_company_id = NULL;
  string_t gdpr_consent_cookie = NULL;
  bool_t subject_to_gdpr = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  sfixed32_t seat_id = NULL;
  enum_t uid_source = NULL;
  bool_t is_whiteops_scanned = NULL;
  sfixed32_t pred_info = NULL;
  crossdevice_group[length_t] crossdevice_groups = NULL;
  bool_t is_amp = NULL;
  sfixed32_t hb_source = NULL;
  string_t external_campaign_id = NULL;
  log_product_ads log_product_ads = NULL;
  bool_t ss_native_assembly_enabled = NULL;
  real64_t emp = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  sfixed32_t postal_code_ext_id = NULL;
  string_t hashed_ip = NULL;
  string_t external_deal_code = NULL;
  sfixed32_t creative_duration = NULL;
  string_t openrtb_req_subdomain = NULL;
  sfixed32_t creative_media_subtype_id = NULL;
  bool_t is_private_auction = NULL;
  bool_t private_auction_eligible = NULL;
  string_t client_request_id = NULL;
  enum_t chrome_traffic_label = NULL;
end;
type log_trade_agreement_info =
record
  sfixed32_t applied_term_id = NULL;
  enum_t applied_term_type = NULL;
  sfixed32_t[length_t] targeted_term_ids = NULL;
end;
type log_impbus_impressions_pricing =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  real64_t buyer_spend = NULL;
  real64_t seller_revenue = NULL;
  sfixed32_t rate_card_auction_type = NULL;
  sfixed32_t rate_card_media_type = NULL;
  bool_t direct_clear = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t instance_id = NULL;
  bool_t two_phase_reduction_applied = NULL;
  sfixed32_t trade_agreement_id = NULL;
  fixed64_t log_timestamp = NULL;
  log_trade_agreement_info trade_agreement_info = NULL;
  bool_t is_buy_it_now = NULL;
  real64_t net_buyer_spend = NULL;
  transaction_event_pricing impression_event_pricing = NULL;
  enum_t counterparty_ruleset_type = NULL;
  real32_t estimated_audience_imps = NULL;
  real32_t audience_imps = NULL;
end;
type location =
record
  real32_t lat = NULL;
  real32_t lon = NULL;
end;
type mobile =
record
  string_t device_unique_id = NULL;
  sfixed32_t device_id = NULL;
  location mobile_location = NULL;
end;
type log_impbus_imps_seen =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  sfixed32_t tag_id = NULL;
  sfixed32_t venue_id = NULL;
  string_t site_domain = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  string_t geo_country = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  sfixed32_t imp_bid_on = NULL;
  string_t call_type = NULL;
  sfixed32_t inventory_source_id = NULL;
  sfixed32_t cleared_direct = NULL;
  sfixed32_t fold_position = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_dw = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t datacenter_id = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t audit_type = NULL;
  sfixed32_t user_group_id = NULL;
  real64_t sampling_pct = NULL;
  sfixed32_t visibility_profile_id = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t cookie_age = NULL;
  string_t ip_address = NULL;
  sfixed32_t truncate_ip = NULL;
  string_t application_id = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t device_type = NULL;
  sfixed32_t operating_system_family_id = NULL;
  mobile mobile = NULL;
  bool_t is_imp_rejecter_applied = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t is_prebid = NULL;
  string_t gdpr_consent_cookie = NULL;
  bool_t subject_to_gdpr = NULL;
  string_t sdk_version = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  sfixed32_t pred_info = NULL;
  bool_t is_amp = NULL;
  sfixed32_t hb_source = NULL;
  bool_t ss_native_assembly_enabled = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  enum_t uid_source = NULL;
  string_t openrtb_req_subdomain = NULL;
  bool_t is_private_auction = NULL;
  bool_t private_auction_eligible = NULL;
  string_t client_request_id = NULL;
  enum_t chrome_traffic_label = NULL;
end;
type log_impbus_preempt =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t imp_transacted = NULL;
  real64_t buyer_spend = NULL;
  real64_t seller_revenue = NULL;
  real64_t bidder_fees = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t fold_position = NULL;
  real64_t seller_deduction = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t creative_id = NULL;
  sfixed32_t cleared_direct = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_exchange_rate = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t creative_audit_status = NULL;
  sfixed32_t is_creative_hosted = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t vp_expose_categories = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  sfixed32_t vp_expose_tag = NULL;
  sfixed32_t bidder_id = NULL;
  sfixed32_t deal_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_dw = NULL;
  sfixed64_t vp_bitmap = NULL;
  sfixed32_t ttl = NULL;
  sfixed32_t view_detection_enabled = NULL;
  sfixed32_t media_type = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t spend_protection = NULL;
  sfixed32_t viewdef_definition_id_buyer_member = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  sfixed32_t bid_price_type = NULL;
  sfixed32_t spend_protection_pixel_id = NULL;
  string_t ip_address = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  log_transaction_def seller_transaction_def = NULL;
  real64_t buyer_bid = NULL;
  sfixed32_t expected_events = NULL;
  fixed64_t accept_timestamp = NULL;
  string_t external_creative_id = NULL;
  sfixed32_t seat_id = NULL;
  bool_t is_prebid_server = NULL;
  sfixed32_t curated_deal_id = NULL;
  string_t external_campaign_id = NULL;
  string_t trust_id = NULL;
  log_product_ads log_product_ads = NULL;
  fixed64_t external_bidrequest_id = NULL;
  fixed64_t external_bidrequest_imp_id = NULL;
  sfixed32_t creative_media_subtype_id = NULL;
end;
type log_impbus_spend_protection =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t verifier_member_id = NULL;
  string_t verifier_detected_domain = NULL;
  sfixed32_t[length_t] reason_ids = NULL;
end;
type log_view_video =
record
  real64_t view_audio_duration_eq_100pct = NULL;
  real64_t view_creative_duration = NULL;
end;
type log_impbus_view =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  fixed64_t user_id_64 = NULL;
  enum_t view_result = NULL;
  sfixed32_t ttl = NULL;
  string_t view_data = NULL;
  sfixed32_t viewdef_definition_id = NULL;
  sfixed32_t viewdef_view_result = NULL;
  sfixed32_t view_not_measurable_type = NULL;
  sfixed32_t view_not_visible_type = NULL;
  sfixed32_t view_frame_type = NULL;
  sfixed32_t view_script_version = NULL;
  string_t view_tag_version = NULL;
  sfixed32_t view_screen_width = NULL;
  sfixed32_t view_screen_height = NULL;
  string_t view_js_browser = NULL;
  string_t view_js_platform = NULL;
  sfixed32_t view_banner_left = NULL;
  sfixed32_t view_banner_top = NULL;
  sfixed32_t view_banner_width = NULL;
  sfixed32_t view_banner_height = NULL;
  real64_t view_tracking_duration = NULL;
  real64_t view_page_duration = NULL;
  real64_t view_usage_duration = NULL;
  real64_t view_surface = NULL;
  string_t view_js_message = NULL;
  sfixed32_t view_player_width = NULL;
  sfixed32_t view_player_height = NULL;
  real64_t view_iab_duration = NULL;
  sfixed32_t view_iab_inview_count = NULL;
  real64_t view_duration_gt_0pct = NULL;
  real64_t view_duration_gt_25pct = NULL;
  real64_t view_duration_gt_50pct = NULL;
  real64_t view_duration_gt_75pct = NULL;
  real64_t view_duration_eq_100pct = NULL;
  fixed64_t auction_timestamp;
  sfixed32_t view_has_banner_left = NULL;
  sfixed32_t view_has_banner_top = NULL;
  sfixed32_t view_mouse_position_final_x = NULL;
  sfixed32_t view_mouse_position_final_y = NULL;
  sfixed32_t view_has_mouse_position_final = NULL;
  sfixed32_t view_mouse_position_initial_x = NULL;
  sfixed32_t view_mouse_position_initial_y = NULL;
  sfixed32_t view_has_mouse_position_initial = NULL;
  sfixed32_t view_mouse_position_page_x = NULL;
  sfixed32_t view_mouse_position_page_y = NULL;
  sfixed32_t view_has_mouse_position_page = NULL;
  sfixed32_t view_mouse_position_timeout_x = NULL;
  sfixed32_t view_mouse_position_timeout_y = NULL;
  sfixed32_t view_has_mouse_position_timeout = NULL;
  fixed64_t view_session_id = NULL;
  log_view_video view_video = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  bool_t is_deferred = NULL;
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
type stage_seen_denormalized =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  sfixed32_t seller_member_id = NULL;
  sfixed32_t buyer_member_id = NULL;
  sfixed32_t width = NULL;
  sfixed32_t height = NULL;
  sfixed32_t publisher_id = NULL;
  sfixed32_t site_id = NULL;
  sfixed32_t tag_id = NULL;
  string_t gender = NULL;
  string_t geo_country = NULL;
  sfixed32_t inventory_source_id = NULL;
  enum_t imp_type = NULL;
  sfixed32_t is_dw = NULL;
  sfixed32_t bidder_id = NULL;
  real64_t sampling_pct = NULL;
  real64_t seller_revenue = NULL;
  real64_t buyer_spend = NULL;
  sfixed32_t cleared_direct = NULL;
  real64_t creative_overage_fees = NULL;
  real64_t auction_service_fees = NULL;
  real64_t clear_fees = NULL;
  real64_t discrepancy_allowance = NULL;
  real64_t forex_allowance = NULL;
  real64_t auction_service_deduction = NULL;
  sfixed32_t content_category_id = NULL;
  sfixed32_t datacenter_id = NULL;
  sfixed32_t imp_bid_on = NULL;
  real64_t ecp = NULL;
  real64_t eap = NULL;
  string_t buyer_currency = NULL;
  real64_t buyer_spend_buyer_currency = NULL;
  string_t seller_currency = NULL;
  real64_t seller_revenue_seller_currency = NULL;
  sfixed32_t vp_expose_pubs = NULL;
  real64_t seller_deduction = NULL;
  sfixed32_t supply_type = NULL;
  sfixed32_t is_delivered = NULL;
  sfixed32_t buyer_bid_bucket = NULL;
  sfixed32_t device_id = NULL;
  fixed64_t user_id_64 = NULL;
  sfixed32_t cookie_age = NULL;
  string_t ip_address = NULL;
  sfixed32_t imp_blacklist_or_fraud = NULL;
  string_t site_domain = NULL;
  sfixed32_t view_measurable = NULL;
  sfixed32_t viewable = NULL;
  string_t call_type = NULL;
  sfixed32_t deal_id = NULL;
  string_t application_id = NULL;
  fixed64_t imp_date_time = NULL;
  sfixed32_t media_type = NULL;
  sfixed32_t pub_rule_id = NULL;
  sfixed32_t venue_id = NULL;
  member_pricing_term buyer_charges = NULL;
  member_pricing_term seller_charges = NULL;
  real64_t buyer_bid = NULL;
  string_t preempt_ip_address = NULL;
  log_transaction_def seller_transaction_def = NULL;
  log_transaction_def buyer_transaction_def = NULL;
  bool_t is_imp_rejecter_applied = NULL;
  bool_t imp_rejecter_do_auction = NULL;
  sfixed32_t audit_type = NULL;
  sfixed32_t browser = NULL;
  sfixed32_t device_type = NULL;
  string_t geo_region = NULL;
  sfixed32_t language = NULL;
  sfixed32_t operating_system = NULL;
  sfixed32_t operating_system_family_id = NULL;
  sfixed32_t[length_t] allowed_media_types = NULL;
  bool_t imp_biddable = NULL;
  bool_t imp_ignored = NULL;
  sfixed32_t user_group_id = NULL;
  sfixed32_t inventory_url_id = NULL;
  sfixed32_t vp_expose_domains = NULL;
  sfixed32_t visibility_profile_id = NULL;
  sfixed32_t is_exclusive = NULL;
  sfixed32_t truncate_ip = NULL;
  sfixed32_t creative_id = NULL;
  real64_t buyer_exchange_rate = NULL;
  sfixed32_t vp_expose_tag = NULL;
  enum_t view_result = NULL;
  sfixed32_t age = NULL;
  sfixed32_t brand_id = NULL;
  sfixed32_t carrier_id = NULL;
  sfixed32_t city = NULL;
  sfixed32_t dma = NULL;
  string_t device_unique_id = NULL;
  string_t latitude = NULL;
  string_t longitude = NULL;
  string_t postal = NULL;
  string_t sdk_version = NULL;
  sfixed32_t pricing_media_type = NULL;
  string_t traffic_source_code = NULL;
  bool_t is_prebid = NULL;
  bool_t is_unit_of_buyer_trx = NULL;
  bool_t is_unit_of_seller_trx = NULL;
  bool_t two_phase_reduction_applied = NULL;
  sfixed32_t region_id = NULL;
  personal_data personal_data = NULL;
  log_anonymized_user_info anonymized_user_info = NULL;
  string_t gdpr_consent_cookie = NULL;
  string_t external_creative_id = NULL;
  bool_t subject_to_gdpr = NULL;
  sfixed32_t fx_rate_snapshot_id = NULL;
  sfixed32_t view_detection_enabled = NULL;
  real64_t seller_exchange_rate = NULL;
  sfixed32_t browser_code_id = NULL;
  sfixed32_t is_prebid_server_included = NULL;
  sfixed32_t bidder_seat_id = NULL;
  string_t default_referrer_url = NULL;
  sfixed32_t pred_info = NULL;
  sfixed32_t curated_deal_id = NULL;
  sfixed32_t deal_type = NULL;
  sfixed32_t primary_height = NULL;
  sfixed32_t primary_width = NULL;
  sfixed32_t curator_member_id = NULL;
  sfixed32_t instance_id = NULL;
  sfixed32_t hb_source = NULL;
  bool_t from_imps_seen = NULL;
  string_t external_campaign_id = NULL;
  bool_t ss_native_assembly_enabled = NULL;
  enum_t uid_source = NULL;
  enum_t video_context = NULL;
  log_personal_identifier[length_t] personal_identifiers = NULL;
  log_personal_identifier[length_t] personal_identifiers_experimental = NULL;
  sfixed32_t user_tz_offset = NULL;
  fixed64_t external_bidrequest_id = NULL;
  fixed64_t external_bidrequest_imp_id = NULL;
  sfixed32_t ym_floor_id = NULL;
  sfixed32_t ym_bias_id = NULL;
  string_t openrtb_req_subdomain = NULL;
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
type quarantine_info =
record
  enum_t reason;
end;
type stage_invalid_impressions_quarantine =
record
  fixed64_t date_time;
  fixed64_t auction_id_64;
  log_dw_bid[length_t] bids = NULL;
  log_dw_view[length_t] dw_views = NULL;
  log_impbus_auction_event[length_t] auction_events = NULL;
  log_impbus_impressions[length_t] impressions = NULL;
  log_impbus_impressions_pricing[length_t] pricings = NULL;
  log_impbus_imps_seen[length_t] imps_seen = NULL;
  log_impbus_preempt[length_t] preempts = NULL;
  log_impbus_spend_protection[length_t] spend_protections = NULL;
  log_impbus_view[length_t] impbus_views = NULL;
  stage_seen_denormalized[length_t] stage_seen_out = NULL;
  agg_dw_impressions[length_t] dw_imps_out = NULL;
  quarantine_info[length_t] quarantine_reasons = NULL;
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
end[int] stage_invalid_impressions_quarantine_message_types =
NULL;
metadata type = stage_invalid_impressions_quarantine ;"""
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
