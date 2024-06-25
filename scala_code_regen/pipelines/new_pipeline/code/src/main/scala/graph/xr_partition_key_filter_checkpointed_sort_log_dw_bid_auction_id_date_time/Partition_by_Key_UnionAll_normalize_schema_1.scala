package graph.xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Partition_by_Key_UnionAll_normalize_schema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("price").cast(DoubleType).as("price"),
      col("member_id").cast(IntegerType).as("member_id"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("creative_freq").cast(IntegerType).as("creative_freq"),
      col("creative_rec").cast(IntegerType).as("creative_rec"),
      col("advertiser_freq").cast(IntegerType).as("advertiser_freq"),
      col("advertiser_rec").cast(IntegerType).as("advertiser_rec"),
      col("is_remarketing").cast(IntegerType).as("is_remarketing"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("media_buy_cost").cast(DoubleType).as("media_buy_cost"),
      col("is_default").cast(IntegerType).as("is_default"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      col("media_buy_rev_share_pct")
        .cast(DoubleType)
        .as("media_buy_rev_share_pct"),
      col("pricing_type").cast(StringType).as("pricing_type"),
      col("can_convert").cast(IntegerType).as("can_convert"),
      col("is_control").cast(IntegerType).as("is_control"),
      col("control_pct").cast(DoubleType).as("control_pct"),
      col("control_creative_id").cast(IntegerType).as("control_creative_id"),
      col("cadence_modifier").cast(DoubleType).as("cadence_modifier"),
      col("advertiser_currency").cast(StringType).as("advertiser_currency"),
      col("advertiser_exchange_rate")
        .cast(DoubleType)
        .as("advertiser_exchange_rate"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("predict_type").cast(IntegerType).as("predict_type"),
      col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
      col("revenue_value_dollars").cast(DoubleType).as("revenue_value_dollars"),
      col("revenue_value_adv_curr")
        .cast(DoubleType)
        .as("revenue_value_adv_curr"),
      col("commission_cpm").cast(DoubleType).as("commission_cpm"),
      col("commission_revshare").cast(DoubleType).as("commission_revshare"),
      col("serving_fees_cpm").cast(DoubleType).as("serving_fees_cpm"),
      col("serving_fees_revshare").cast(DoubleType).as("serving_fees_revshare"),
      col("publisher_currency").cast(StringType).as("publisher_currency"),
      col("publisher_exchange_rate")
        .cast(DoubleType)
        .as("publisher_exchange_rate"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      col("payment_value").cast(DoubleType).as("payment_value"),
      col("creative_group_freq").cast(IntegerType).as("creative_group_freq"),
      col("creative_group_rec").cast(IntegerType).as("creative_group_rec"),
      col("revenue_type").cast(IntegerType).as("revenue_type"),
      col("apply_cost_on_default")
        .cast(IntegerType)
        .as("apply_cost_on_default"),
      col("instance_id").cast(IntegerType).as("instance_id"),
      col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
      col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
      col("targeted_segments").cast(StringType).as("targeted_segments"),
      col("ttl").cast(IntegerType).as("ttl"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("data_costs")
        .cast(
          ArrayType(
            StructType(
              Array(
                StructField("data_member_id", IntegerType, true),
                StructField("cost",           DoubleType,  true),
                StructField("used_segments",
                            ArrayType(IntegerType, true),
                            true
                ),
                StructField("cost_pct", DoubleType, true)
              )
            ),
            true
          )
        )
        .as("data_costs"),
      col("targeted_segment_list")
        .cast(ArrayType(IntegerType, true))
        .as("targeted_segment_list"),
      col("campaign_group_freq").cast(IntegerType).as("campaign_group_freq"),
      col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
      col("insertion_order_freq").cast(IntegerType).as("insertion_order_freq"),
      col("insertion_order_rec").cast(IntegerType).as("insertion_order_rec"),
      col("buyer_gender").cast(StringType).as("buyer_gender"),
      col("buyer_age").cast(IntegerType).as("buyer_age"),
      col("custom_model_id").cast(IntegerType).as("custom_model_id"),
      col("custom_model_last_modified")
        .cast(LongType)
        .as("custom_model_last_modified"),
      col("custom_model_output_code")
        .cast(StringType)
        .as("custom_model_output_code"),
      col("bid_priority").cast(IntegerType).as("bid_priority"),
      col("explore_disposition").cast(IntegerType).as("explore_disposition"),
      col("revenue_auction_event_type")
        .cast(IntegerType)
        .as("revenue_auction_event_type"),
      col("campaign_group_models")
        .cast(
          ArrayType(
            StructType(
              Array(
                StructField("model_type", IntegerType, true),
                StructField("model_id",   IntegerType, true),
                StructField("leaf_code",  StringType,  true),
                StructField("origin",     IntegerType, true),
                StructField("experiment", IntegerType, true),
                StructField("value",      FloatType,   true)
              )
            ),
            true
          )
        )
        .as("campaign_group_models"),
      col("impression_transaction_type")
        .cast(IntegerType)
        .as("impression_transaction_type"),
      col("is_deferred").cast(IntegerType).as("is_deferred"),
      col("log_type").cast(IntegerType).as("log_type"),
      when(
        is_not_null(col("crossdevice_group_anon")),
        struct(col("crossdevice_group_anon.graph_id").as("graph_id"),
               col("crossdevice_group_anon.group_id").as("group_id")
        )
      ).cast(
          StructType(
            Array(StructField("graph_id", IntegerType, true),
                  StructField("group_id", BinaryType,  true)
            )
          )
        )
        .as("crossdevice_group_anon"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      when(
        is_not_null(col("crossdevice_graph_cost")),
        struct(col("crossdevice_graph_cost.graph_provider_member_id").as(
                 "graph_provider_member_id"
               ),
               col("crossdevice_graph_cost.cost_cpm_usd").as("cost_cpm_usd")
        )
      ).cast(
          StructType(
            Array(StructField("graph_provider_member_id", IntegerType, true),
                  StructField("cost_cpm_usd",             DoubleType,  true)
            )
          )
        )
        .as("crossdevice_graph_cost"),
      col("revenue_event_type_id")
        .cast(IntegerType)
        .as("revenue_event_type_id"),
      col("targeted_segment_details")
        .cast(
          ArrayType(StructType(
                      Array(StructField("segment_id",    IntegerType, true),
                            StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("targeted_segment_details"),
      col("insertion_order_budget_interval_id")
        .cast(IntegerType)
        .as("insertion_order_budget_interval_id"),
      col("campaign_group_budget_interval_id")
        .cast(IntegerType)
        .as("campaign_group_budget_interval_id"),
      col("cold_start_price_type")
        .cast(IntegerType)
        .as("cold_start_price_type"),
      col("discovery_state").cast(IntegerType).as("discovery_state"),
      when(
        is_not_null(col("revenue_info")),
        struct(
          col("revenue_info.total_partner_fees_microcents").as(
            "total_partner_fees_microcents"
          ),
          col("revenue_info.booked_revenue_dollars").as(
            "booked_revenue_dollars"
          ),
          col("revenue_info.booked_revenue_adv_curr").as(
            "booked_revenue_adv_curr"
          ),
          col("revenue_info.total_data_costs_microcents").as(
            "total_data_costs_microcents"
          ),
          col("revenue_info.total_profit_microcents").as(
            "total_profit_microcents"
          ),
          col("revenue_info.total_segment_data_costs_microcents").as(
            "total_segment_data_costs_microcents"
          ),
          col("revenue_info.total_feature_costs_microcents").as(
            "total_feature_costs_microcents"
          )
        )
      ).cast(
          StructType(
            Array(
              StructField("total_partner_fees_microcents", LongType,   true),
              StructField("booked_revenue_dollars",        DoubleType, true),
              StructField("booked_revenue_adv_curr",       DoubleType, true),
              StructField("total_data_costs_microcents",   LongType,   true),
              StructField("total_profit_microcents",       LongType,   true),
              StructField("total_segment_data_costs_microcents",
                          LongType,
                          true
              ),
              StructField("total_feature_costs_microcents", LongType, true)
            )
          )
        )
        .as("revenue_info"),
      col("use_revenue_info").cast(BooleanType).as("use_revenue_info"),
      col("sales_tax_rate_pct").cast(DoubleType).as("sales_tax_rate_pct"),
      col("targeted_crossdevice_graph_id")
        .cast(IntegerType)
        .as("targeted_crossdevice_graph_id"),
      col("product_feed_id").cast(IntegerType).as("product_feed_id"),
      col("item_selection_strategy_id")
        .cast(IntegerType)
        .as("item_selection_strategy_id"),
      col("discovery_prediction").cast(DoubleType).as("discovery_prediction"),
      col("bidding_host_id").cast(IntegerType).as("bidding_host_id"),
      col("split_id").cast(IntegerType).as("split_id"),
      col("excluded_targeted_segment_details")
        .cast(
          ArrayType(
            StructType(Array(StructField("segment_id", IntegerType, true))),
            true
          )
        )
        .as("excluded_targeted_segment_details"),
      col("predicted_kpi_event_rate")
        .cast(DoubleType)
        .as("predicted_kpi_event_rate"),
      col("has_crossdevice_reach_extension")
        .cast(BooleanType)
        .as("has_crossdevice_reach_extension"),
      col("advertiser_expected_value_ecpm_ac")
        .cast(DoubleType)
        .as("advertiser_expected_value_ecpm_ac"),
      col("bpp_multiplier").cast(DoubleType).as("bpp_multiplier"),
      col("bpp_offset").cast(DoubleType).as("bpp_offset"),
      col("bid_modifier").cast(DoubleType).as("bid_modifier"),
      col("payment_value_microcents")
        .cast(LongType)
        .as("payment_value_microcents"),
      col("crossdevice_graph_membership")
        .cast(
          ArrayType(StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", BinaryType,  true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_graph_membership"),
      col("valuation_landscape")
        .cast(
          ArrayType(
            StructType(
              Array(
                StructField("kpi_event_id",               IntegerType, true),
                StructField("ev_kpi_event_ac",            DoubleType,  true),
                StructField("p_kpi_event",                DoubleType,  true),
                StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                StructField("min_margin_pct",             DoubleType,  true),
                StructField("max_revenue_or_bid_value",   DoubleType,  true),
                StructField("min_revenue_or_bid_value",   DoubleType,  true),
                StructField("cold_start_price_ac",        DoubleType,  true),
                StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                StructField("p_revenue_event",            DoubleType,  true),
                StructField("total_fees_deducted_ac",     DoubleType,  true)
              )
            ),
            true
          )
        )
        .as("valuation_landscape"),
      col("line_item_currency").cast(StringType).as("line_item_currency"),
      col("measurement_fee_cpm_usd")
        .cast(DoubleType)
        .as("measurement_fee_cpm_usd"),
      col("measurement_provider_id")
        .cast(IntegerType)
        .as("measurement_provider_id"),
      col("measurement_provider_member_id")
        .cast(IntegerType)
        .as("measurement_provider_member_id"),
      col("offline_attribution_provider_member_id")
        .cast(IntegerType)
        .as("offline_attribution_provider_member_id"),
      col("offline_attribution_cost_usd_cpm")
        .cast(DoubleType)
        .as("offline_attribution_cost_usd_cpm"),
      col("targeted_segment_details_by_id_type")
        .cast(
          ArrayType(
            StructType(
              Array(
                StructField("identity_type", IntegerType, true),
                StructField(
                  "targeted_segment_details",
                  ArrayType(
                    StructType(
                      Array(StructField("segment_id",    IntegerType, true),
                            StructField("last_seen_min", IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                )
              )
            ),
            true
          )
        )
        .as("targeted_segment_details_by_id_type"),
      col("offline_attribution")
        .cast(
          ArrayType(
            StructType(
              Array(StructField("provider_member_id", IntegerType, true),
                    StructField("cost_usd_cpm",       DoubleType,  true)
              )
            ),
            true
          )
        )
        .as("offline_attribution"),
      col("frequency_cap_type_internal")
        .cast(IntegerType)
        .as("frequency_cap_type_internal"),
      col("modeled_cap_did_override_line_item_daily_cap")
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      col("modeled_cap_user_sample_rate")
        .cast(DoubleType)
        .as("modeled_cap_user_sample_rate"),
      col("bid_rate").cast(DoubleType).as("bid_rate"),
      col("district_postal_code_lists")
        .cast(ArrayType(IntegerType, true))
        .as("district_postal_code_lists"),
      col("pre_bpp_price").cast(DoubleType).as("pre_bpp_price"),
      col("feature_tests_bitmap").cast(IntegerType).as("feature_tests_bitmap")
    )

}
