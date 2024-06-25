package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_dw_bid_pb_log_dw_bid {

  def apply(context: Context): DataFrame = {
    val spark = context.spark
    import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
    import _root_.io.prophecy.libs.FFSchemaRecord
    import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json
    var df: DataFrame = spark.emptyDataFrame
    try {
      var reader = spark.read
        .option(
          "schema",
          Some("""type fixed32_t = unsigned little endian integer(4);
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
end[int] log_dw_bid_message_types =
[vector
  [record
    name "data_cost"
    field_infos [vector
                  [record name "data_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost" number 2 type_code 1 message_type -1 optional 1],
                  [record name "used_segments" number 3 type_code 20 message_type -1 optional 1],
                  [record name "cost_pct" number 4 type_code 1 message_type -1 optional 1]]],
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
    name "revenue_info"
    field_infos [vector
                  [record name "total_partner_fees_microcents" number 1 type_code 3 message_type -1 optional 1],
                  [record name "booked_revenue_dollars" number 2 type_code 1 message_type -1 optional 1],
                  [record name "booked_revenue_adv_curr" number 3 type_code 1 message_type -1 optional 1],
                  [record name "total_data_costs_microcents" number 4 type_code 3 message_type -1 optional 1],
                  [record name "total_profit_microcents" number 5 type_code 3 message_type -1 optional 1],
                  [record name "total_segment_data_costs_microcents" number 6 type_code 3 message_type -1 optional 1],
                  [record name "total_feature_costs_microcents" number 7 type_code 3 message_type -1 optional 1]]],
  [record
    name "excluded_targeted_segment_details"
    field_infos [vector
                  [record name "segment_id" number 1 type_code 4 message_type -1 optional 1]]],
  [record
    name "valuation_landscape"
    field_infos [vector
                  [record name "kpi_event_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "ev_kpi_event_ac" number 2 type_code 1 message_type -1 optional 1],
                  [record name "p_kpi_event" number 3 type_code 1 message_type -1 optional 1],
                  [record name "bpo_aggressiveness_factor" number 4 type_code 1 message_type -1 optional 1],
                  [record name "min_margin_pct" number 5 type_code 1 message_type -1 optional 1],
                  [record name "max_revenue_or_bid_value" number 6 type_code 1 message_type -1 optional 1],
                  [record name "min_revenue_or_bid_value" number 7 type_code 1 message_type -1 optional 1],
                  [record name "cold_start_price_ac" number 8 type_code 1 message_type -1 optional 1],
                  [record name "dynamic_bid_max_revenue_ac" number 9 type_code 1 message_type -1 optional 1],
                  [record name "p_revenue_event" number 10 type_code 1 message_type -1 optional 1],
                  [record name "total_fees_deducted_ac" number 11 type_code 1 message_type -1 optional 1]]],
  [record
    name "targeted_segment_details_by_id_type"
    field_infos [vector
                  [record name "identity_type" number 1 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segment_details" number 2 type_code 25 message_type 4 optional 1]]],
  [record
    name "offline_attribution"
    field_infos [vector
                  [record name "provider_member_id" number 1 type_code 4 message_type -1 optional 1],
                  [record name "cost_usd_cpm" number 2 type_code 1 message_type -1 optional 1]]],
  [record
    name "log_dw_bid"
    field_infos [vector
                  [record name "date_time" number 1 type_code 1 message_type -1 optional 0],
                  [record name "auction_id_64" number 2 type_code 1 message_type -1 optional 0],
                  [record name "price" number 3 type_code 1 message_type -1 optional 1],
                  [record name "member_id" number 4 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_id" number 5 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_id" number 6 type_code 4 message_type -1 optional 1],
                  [record name "campaign_id" number 7 type_code 4 message_type -1 optional 1],
                  [record name "creative_id" number 8 type_code 4 message_type -1 optional 1],
                  [record name "creative_freq" number 9 type_code 4 message_type -1 optional 1],
                  [record name "creative_rec" number 10 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_freq" number 11 type_code 4 message_type -1 optional 1],
                  [record name "advertiser_rec" number 12 type_code 4 message_type -1 optional 1],
                  [record name "is_remarketing" number 13 type_code 4 message_type -1 optional 1],
                  [record name "user_group_id" number 14 type_code 4 message_type -1 optional 1],
                  [record name "media_buy_cost" number 15 type_code 1 message_type -1 optional 1],
                  [record name "is_default" number 16 type_code 4 message_type -1 optional 1],
                  [record name "pub_rule_id" number 17 type_code 4 message_type -1 optional 1],
                  [record name "media_buy_rev_share_pct" number 18 type_code 1 message_type -1 optional 1],
                  [record name "pricing_type" number 19 type_code 8 message_type -1 optional 1],
                  [record name "can_convert" number 20 type_code 4 message_type -1 optional 1],
                  [record name "is_control" number 21 type_code 4 message_type -1 optional 1],
                  [record name "control_pct" number 22 type_code 1 message_type -1 optional 1],
                  [record name "control_creative_id" number 23 type_code 4 message_type -1 optional 1],
                  [record name "cadence_modifier" number 24 type_code 1 message_type -1 optional 1],
                  [record name "advertiser_currency" number 25 type_code 8 message_type -1 optional 1],
                  [record name "advertiser_exchange_rate" number 26 type_code 1 message_type -1 optional 1],
                  [record name "insertion_order_id" number 27 type_code 4 message_type -1 optional 1],
                  [record name "predict_type" number 28 type_code 4 message_type -1 optional 1],
                  [record name "predict_type_goal" number 29 type_code 4 message_type -1 optional 1],
                  [record name "revenue_value_dollars" number 30 type_code 1 message_type -1 optional 1],
                  [record name "revenue_value_adv_curr" number 31 type_code 1 message_type -1 optional 1],
                  [record name "commission_cpm" number 32 type_code 1 message_type -1 optional 1],
                  [record name "commission_revshare" number 33 type_code 1 message_type -1 optional 1],
                  [record name "serving_fees_cpm" number 34 type_code 1 message_type -1 optional 1],
                  [record name "serving_fees_revshare" number 35 type_code 1 message_type -1 optional 1],
                  [record name "publisher_currency" number 36 type_code 8 message_type -1 optional 1],
                  [record name "publisher_exchange_rate" number 37 type_code 1 message_type -1 optional 1],
                  [record name "payment_type" number 38 type_code 4 message_type -1 optional 1],
                  [record name "payment_value" number 39 type_code 1 message_type -1 optional 1],
                  [record name "creative_group_freq" number 40 type_code 4 message_type -1 optional 1],
                  [record name "creative_group_rec" number 41 type_code 4 message_type -1 optional 1],
                  [record name "revenue_type" number 42 type_code 4 message_type -1 optional 1],
                  [record name "apply_cost_on_default" number 43 type_code 4 message_type -1 optional 1],
                  [record name "instance_id" number 44 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_age" number 45 type_code 4 message_type -1 optional 1],
                  [record name "vp_expose_gender" number 46 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segments" number 47 type_code 8 message_type -1 optional 1],
                  [record name "ttl" number 48 type_code 4 message_type -1 optional 1],
                  [record name "auction_timestamp" number 49 type_code 1 message_type -1 optional 0],
                  [record name "data_costs" number 50 type_code 25 message_type 0 optional 1],
                  [record name "targeted_segment_list" number 51 type_code 20 message_type -1 optional 1],
                  [record name "campaign_group_freq" number 52 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_rec" number 53 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_freq" number 54 type_code 4 message_type -1 optional 1],
                  [record name "insertion_order_rec" number 55 type_code 4 message_type -1 optional 1],
                  [record name "buyer_gender" number 56 type_code 8 message_type -1 optional 1],
                  [record name "buyer_age" number 57 type_code 4 message_type -1 optional 1],
                  [record name "custom_model_id" number 58 type_code 4 message_type -1 optional 1],
                  [record name "custom_model_last_modified" number 59 type_code 1 message_type -1 optional 1],
                  [record name "custom_model_output_code" number 60 type_code 8 message_type -1 optional 1],
                  [record name "bid_priority" number 61 type_code 4 message_type -1 optional 1],
                  [record name "explore_disposition" number 62 type_code 4 message_type -1 optional 1],
                  [record name "revenue_auction_event_type" number 63 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_models" number 64 type_code 25 message_type 1 optional 1],
                  [record name "impression_transaction_type" number 65 type_code 4 message_type -1 optional 1],
                  [record name "is_deferred" number 68 type_code 4 message_type -1 optional 1],
                  [record name "log_type" number 69 type_code 4 message_type -1 optional 1],
                  [record name "crossdevice_group_anon" number 73 type_code 9 message_type 2 optional 1],
                  [record name "fx_rate_snapshot_id" number 74 type_code 4 message_type -1 optional 1],
                  [record name "crossdevice_graph_cost" number 75 type_code 9 message_type 3 optional 1],
                  [record name "revenue_event_type_id" number 76 type_code 4 message_type -1 optional 1],
                  [record name "targeted_segment_details" number 77 type_code 25 message_type 4 optional 1],
                  [record name "insertion_order_budget_interval_id" number 78 type_code 4 message_type -1 optional 1],
                  [record name "campaign_group_budget_interval_id" number 79 type_code 4 message_type -1 optional 1],
                  [record name "cold_start_price_type" number 80 type_code 4 message_type -1 optional 1],
                  [record name "discovery_state" number 81 type_code 4 message_type -1 optional 1],
                  [record name "revenue_info" number 82 type_code 9 message_type 5 optional 1],
                  [record name "use_revenue_info" number 83 type_code 7 message_type -1 optional 1],
                  [record name "sales_tax_rate_pct" number 84 type_code 1 message_type -1 optional 1],
                  [record name "targeted_crossdevice_graph_id" number 85 type_code 4 message_type -1 optional 1],
                  [record name "product_feed_id" number 86 type_code 4 message_type -1 optional 1],
                  [record name "item_selection_strategy_id" number 87 type_code 4 message_type -1 optional 1],
                  [record name "discovery_prediction" number 88 type_code 1 message_type -1 optional 1],
                  [record name "bidding_host_id" number 89 type_code 4 message_type -1 optional 1],
                  [record name "split_id" number 90 type_code 4 message_type -1 optional 1],
                  [record name "excluded_targeted_segment_details" number 91 type_code 25 message_type 6 optional 1],
                  [record name "predicted_kpi_event_rate" number 92 type_code 1 message_type -1 optional 1],
                  [record name "has_crossdevice_reach_extension" number 93 type_code 7 message_type -1 optional 1],
                  [record name "advertiser_expected_value_ecpm_ac" number 94 type_code 1 message_type -1 optional 1],
                  [record name "bpp_multiplier" number 95 type_code 1 message_type -1 optional 1],
                  [record name "bpp_offset" number 96 type_code 1 message_type -1 optional 1],
                  [record name "bid_modifier" number 97 type_code 1 message_type -1 optional 1],
                  [record name "payment_value_microcents" number 98 type_code 3 message_type -1 optional 1],
                  [record name "crossdevice_graph_membership" number 99 type_code 25 message_type 2 optional 1],
                  [record name "valuation_landscape" number 100 type_code 25 message_type 7 optional 1],
                  [record name "line_item_currency" number 101 type_code 8 message_type -1 optional 1],
                  [record name "measurement_fee_cpm_usd" number 102 type_code 1 message_type -1 optional 1],
                  [record name "measurement_provider_id" number 103 type_code 4 message_type -1 optional 1],
                  [record name "measurement_provider_member_id" number 104 type_code 4 message_type -1 optional 1],
                  [record name "offline_attribution_provider_member_id" number 105 type_code 4 message_type -1 optional 1],
                  [record name "offline_attribution_cost_usd_cpm" number 106 type_code 1 message_type -1 optional 1],
                  [record name "targeted_segment_details_by_id_type" number 107 type_code 25 message_type 8 optional 1],
                  [record name "offline_attribution" number 108 type_code 25 message_type 9 optional 1],
                  [record name "frequency_cap_type_internal" number 109 type_code 4 message_type -1 optional 1],
                  [record name "modeled_cap_did_override_line_item_daily_cap" number 110 type_code 7 message_type -1 optional 1],
                  [record name "modeled_cap_user_sample_rate" number 111 type_code 1 message_type -1 optional 1],
                  [record name "bid_rate" number 112 type_code 1 message_type -1 optional 1],
                  [record name "district_postal_code_lists" number 113 type_code 20 message_type -1 optional 1],
                  [record name "pre_bpp_price" number 115 type_code 1 message_type -1 optional 1],
                  [record name "feature_tests_bitmap" number 116 type_code 4 message_type -1 optional 1]]]];
metadata type = log_dw_bid;
metadata type = "log_dw_bid" ;""")
            .map(s => parse(s).asInstanceOf[FFSchemaRecord])
            .map(s => Json.stringify(Json.toJson(s)))
            .getOrElse("")
        )
        .format("io.prophecy.libs.FixedFileFormat")
      df = reader.load("NA")
    } catch {
      case e: Error =>
        println(s"Error occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
      case e: Throwable =>
        println(s"Throwable occurred while reading dataframe: $e")
        throw new Exception(e.getMessage)
    }
    df
  }

}
