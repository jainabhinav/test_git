package graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Partition_by_Key_UnionAll_normalize_schema_1_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      when(
        is_not_null(col("buyer_charges")),
        struct(
          col("buyer_charges.rate_card_id").as("rate_card_id"),
          col("buyer_charges.member_id").as("member_id"),
          col("buyer_charges.is_dw").as("is_dw"),
          col("buyer_charges.pricing_terms").as("pricing_terms"),
          col("buyer_charges.fx_margin_rate_id").as("fx_margin_rate_id"),
          col("buyer_charges.marketplace_owner_id").as("marketplace_owner_id"),
          col("buyer_charges.virtual_marketplace_id").as(
            "virtual_marketplace_id"
          ),
          col("buyer_charges.amino_enabled").as("amino_enabled")
        )
      ).cast(
          StructType(
            Array(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    Array(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("buyer_charges"),
      when(
        is_not_null(col("seller_charges")),
        struct(
          col("seller_charges.rate_card_id").as("rate_card_id"),
          col("seller_charges.member_id").as("member_id"),
          col("seller_charges.is_dw").as("is_dw"),
          col("seller_charges.pricing_terms").as("pricing_terms"),
          col("seller_charges.fx_margin_rate_id").as("fx_margin_rate_id"),
          col("seller_charges.marketplace_owner_id").as("marketplace_owner_id"),
          col("seller_charges.virtual_marketplace_id").as(
            "virtual_marketplace_id"
          ),
          col("seller_charges.amino_enabled").as("amino_enabled")
        )
      ).cast(
          StructType(
            Array(
              StructField("rate_card_id", IntegerType, true),
              StructField("member_id",    IntegerType, true),
              StructField("is_dw",        BooleanType, true),
              StructField(
                "pricing_terms",
                ArrayType(
                  StructType(
                    Array(
                      StructField("term_id",                 IntegerType, true),
                      StructField("amount",                  DoubleType,  true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("is_deduction",            BooleanType, true),
                      StructField("is_media_cost_dependent", BooleanType, true),
                      StructField("data_member_id",          IntegerType, true)
                    )
                  ),
                  true
                ),
                true
              ),
              StructField("fx_margin_rate_id",      IntegerType, true),
              StructField("marketplace_owner_id",   IntegerType, true),
              StructField("virtual_marketplace_id", IntegerType, true),
              StructField("amino_enabled",          BooleanType, true)
            )
          )
        )
        .as("seller_charges"),
      col("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col("seller_revenue").cast(DoubleType).as("seller_revenue"),
      col("rate_card_auction_type")
        .cast(IntegerType)
        .as("rate_card_auction_type"),
      col("rate_card_media_type").cast(IntegerType).as("rate_card_media_type"),
      col("direct_clear").cast(BooleanType).as("direct_clear"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("instance_id").cast(IntegerType).as("instance_id"),
      col("two_phase_reduction_applied")
        .cast(BooleanType)
        .as("two_phase_reduction_applied"),
      col("trade_agreement_id").cast(IntegerType).as("trade_agreement_id"),
      col("log_timestamp").cast(LongType).as("log_timestamp"),
      when(
        is_not_null(col("trade_agreement_info")),
        struct(
          col("trade_agreement_info.applied_term_id").as("applied_term_id"),
          col("trade_agreement_info.applied_term_type").as("applied_term_type"),
          col("trade_agreement_info.targeted_term_ids").as("targeted_term_ids")
        )
      ).cast(
          StructType(
            Array(
              StructField("applied_term_id",   IntegerType, true),
              StructField("applied_term_type", IntegerType, true),
              StructField("targeted_term_ids",
                          ArrayType(IntegerType, true),
                          true
              )
            )
          )
        )
        .as("trade_agreement_info"),
      col("is_buy_it_now").cast(BooleanType).as("is_buy_it_now"),
      col("net_buyer_spend").cast(DoubleType).as("net_buyer_spend"),
      impression_event_pricing(context).as("impression_event_pricing"),
      col("counterparty_ruleset_type")
        .cast(IntegerType)
        .as("counterparty_ruleset_type"),
      col("estimated_audience_imps")
        .cast(FloatType)
        .as("estimated_audience_imps"),
      col("audience_imps").cast(FloatType).as("audience_imps")
    )

  def impression_event_pricing(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("impression_event_pricing")),
      struct(
        col("impression_event_pricing.gross_payment_value_microcents").as(
          "gross_payment_value_microcents"
        ),
        col("impression_event_pricing.net_payment_value_microcents").as(
          "net_payment_value_microcents"
        ),
        col("impression_event_pricing.seller_revenue_microcents").as(
          "seller_revenue_microcents"
        ),
        struct(
          col("impression_event_pricing.buyer_charges.rate_card_id").as(
            "rate_card_id"
          ),
          col("impression_event_pricing.buyer_charges.member_id").as(
            "member_id"
          ),
          col("impression_event_pricing.buyer_charges.is_dw").as("is_dw"),
          col("impression_event_pricing.buyer_charges.pricing_terms").as(
            "pricing_terms"
          ),
          col("impression_event_pricing.buyer_charges.fx_margin_rate_id").as(
            "fx_margin_rate_id"
          ),
          col("impression_event_pricing.buyer_charges.marketplace_owner_id").as(
            "marketplace_owner_id"
          ),
          col("impression_event_pricing.buyer_charges.virtual_marketplace_id")
            .as("virtual_marketplace_id"),
          col("impression_event_pricing.buyer_charges.amino_enabled").as(
            "amino_enabled"
          )
        ).as("buyer_charges"),
        struct(
          col("impression_event_pricing.seller_charges.rate_card_id").as(
            "rate_card_id"
          ),
          col("impression_event_pricing.seller_charges.member_id").as(
            "member_id"
          ),
          col("impression_event_pricing.seller_charges.is_dw").as("is_dw"),
          col("impression_event_pricing.seller_charges.pricing_terms").as(
            "pricing_terms"
          ),
          col("impression_event_pricing.seller_charges.fx_margin_rate_id").as(
            "fx_margin_rate_id"
          ),
          col("impression_event_pricing.seller_charges.marketplace_owner_id")
            .as("marketplace_owner_id"),
          col("impression_event_pricing.seller_charges.virtual_marketplace_id")
            .as("virtual_marketplace_id"),
          col("impression_event_pricing.seller_charges.amino_enabled").as(
            "amino_enabled"
          )
        ).as("seller_charges"),
        col("impression_event_pricing.buyer_transacted").as("buyer_transacted"),
        col("impression_event_pricing.seller_transacted").as(
          "seller_transacted"
        )
      )
    ).cast(
      StructType(
        Array(
          StructField("gross_payment_value_microcents", LongType, true),
          StructField("net_payment_value_microcents",   LongType, true),
          StructField("seller_revenue_microcents",      LongType, true),
          StructField(
            "buyer_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        BooleanType, true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",      IntegerType, true),
                        StructField("amount",       DoubleType,  true),
                        StructField("rate",         DoubleType,  true),
                        StructField("is_deduction", BooleanType, true),
                        StructField("is_media_cost_dependent",
                                    BooleanType,
                                    true
                        ),
                        StructField("data_member_id", IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          BooleanType, true)
              )
            ),
            true
          ),
          StructField(
            "seller_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        BooleanType, true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",      IntegerType, true),
                        StructField("amount",       DoubleType,  true),
                        StructField("rate",         DoubleType,  true),
                        StructField("is_deduction", BooleanType, true),
                        StructField("is_media_cost_dependent",
                                    BooleanType,
                                    true
                        ),
                        StructField("data_member_id", IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          BooleanType, true)
              )
            ),
            true
          ),
          StructField("buyer_transacted",  BooleanType, true),
          StructField("seller_transacted", BooleanType, true)
        )
      )
    )
  }

}
