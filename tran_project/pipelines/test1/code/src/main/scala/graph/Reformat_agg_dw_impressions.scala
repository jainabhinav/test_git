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

object Reformat_agg_dw_impressions {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        col("log_dw_view.date_time").cast(LongType),
        when(
          (col("in_f_create_agg_dw_impressions.ttl_var") > lit(3600)).and(
            is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
          ),
          col("log_impbus_preempt.date_time").cast(LongType)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      coalesce(
        when(col("log_dw_view.user_id_64").cast(LongType) =!= lit(0),
             col("log_dw_view.user_id_64").cast(LongType)
        ),
        when(col("log_impbus_impressions.user_id_64").cast(LongType) =!= lit(0),
             col("log_impbus_impressions.user_id_64").cast(LongType)
        )
      ).as("user_id_64"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
      when(col("log_impbus_impressions.venue_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.venue_id").cast(IntegerType)
      ).as("venue_id"),
      when(col("log_impbus_impressions.inventory_source_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_source_id").cast(IntegerType)
      ).as("inventory_source_id"),
      lit(null).cast(IntegerType).as("session_frequency"),
      when(string_compare(col("log_impbus_impressions.site_domain"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.site_domain")
      ).as("site_domain"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).as("height"),
      when(string_compare(col("log_impbus_impressions.geo_country"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_country")
      ).as("geo_country"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("geo_region"),
      when(string_compare(col("log_impbus_impressions.gender"),
                          lit("u")
           ) =!= lit(0),
           col("log_impbus_impressions.gender")
      ).as("gender"),
      when(col("log_impbus_impressions.age").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.age").cast(IntegerType)
      ).as("age"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      when(col("imp_type").cast(IntegerType) =!= lit(1),
           col("in_f_create_agg_dw_impressions.buyer_member_id_var")
      ).otherwise(lit(0)).as("buyer_member_id"),
      col("in_f_create_agg_dw_impressions.creative_id_var").as("creative_id"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      when(string_compare(col("log_impbus_impressions.buyer_currency"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.buyer_currency")
      ).as("buyer_currency"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.buyer_bid"),
        col("log_impbus_preempt.buyer_bid")
      ).as("buyer_bid"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var").isin(8, 1, 3),
             lit(0)
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(5),
             col("log_dw_bid.price")
        ),
        col("in_f_create_agg_dw_impressions.buyer_spend_cpm_var")
      ).as("buyer_spend"),
      when(col("log_impbus_impressions.ecp") =!= lit(0),
           col("log_impbus_impressions.ecp")
      ).as("ecp"),
      when(col("log_impbus_impressions.reserve_price") =!= lit(0.0d),
           col("log_impbus_impressions.reserve_price")
      ).as("reserve_price"),
      coalesce(
        when(
          (col("imp_type").cast(IntegerType) === lit(1))
            .or(
              col("in_f_create_agg_dw_impressions.buyer_member_id_var") === lit(
                0
              )
            )
            .and(
              is_not_null(
                col("in_f_create_agg_dw_impressions.advertiser_id_var")
              )
            ),
          lit(0)
        ),
        col("in_f_create_agg_dw_impressions.advertiser_id_var")
      ).as("advertiser_id"),
      coalesce(
        when(
          (col("imp_type").cast(IntegerType) === lit(1))
            .or(
              col("in_f_create_agg_dw_impressions.buyer_member_id_var") === lit(
                0
              )
            )
            .and(
              is_not_null(
                col("in_f_create_agg_dw_impressions.campaign_group_id_var")
              )
            ),
          lit(0)
        ),
        col("in_f_create_agg_dw_impressions.campaign_group_id_var")
      ).as("campaign_group_id"),
      coalesce(
        when(
          (col("imp_type").cast(IntegerType) === lit(1))
            .or(
              col("in_f_create_agg_dw_impressions.buyer_member_id_var") === lit(
                0
              )
            )
            .and(
              is_not_null(col("in_f_create_agg_dw_impressions.campaign_id_var"))
            ),
          lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") =!= lit(6)).and(
            col("in_f_create_agg_dw_impressions.campaign_id_var") =!= lit(0)
          ),
          col("in_f_create_agg_dw_impressions.campaign_id_var")
        )
      ).as("campaign_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.creative_freq").cast(IntegerType) =!= lit(0)),
          col("log_dw_bid.creative_freq").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_creative_freq")
        )
      ).as("creative_freq"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.creative_rec").cast(IntegerType) =!= lit(0)),
          col("log_dw_bid.creative_rec").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_creative_rec")
        )
      ).as("creative_rec"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.predict_type").cast(IntegerType) === lit(1)),
          lit(1)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              (col("log_dw_bid.predict_type").cast(IntegerType) === lit(0)).or(
                (col("log_dw_bid.predict_type").cast(IntegerType) >= lit(2))
                  .and(
                    col("log_dw_bid.predict_type").cast(IntegerType) =!= lit(9)
                  )
              )
            ),
          lit(2)
        ),
        lit(0)
      ).as("is_learn"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.is_remarketing").cast(IntegerType) =!= lit(0)),
        col("log_dw_bid.is_remarketing").cast(IntegerType)
      ).as("is_remarketing"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.advertiser_freq").cast(IntegerType) =!= lit(0)
            ),
          col("log_dw_bid.advertiser_freq").cast(IntegerType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(9)),
          lit(0)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_advertiser_freq"
          )
        )
      ).as("advertiser_frequency"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.advertiser_rec").cast(IntegerType) =!= lit(0)),
          col("log_dw_bid.advertiser_rec").cast(IntegerType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(9)),
          lit(0)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_advertiser_rec"
          )
        )
      ).as("advertiser_recency"),
      coalesce(
        when(col("log_impbus_impressions.user_id_64").cast(LongType) > lit(
               9223372036854775807L
             ).cast(LongType),
             lit(-1)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.is_placeholder_bid_var") === lit(
            1
          )).or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(8))
            .and(
              is_not_null(
                col("log_impbus_impressions.user_id_64").cast(LongType)
              )
            ),
          col("log_impbus_impressions.user_id_64").cast(LongType) % lit(1000)
        ),
        when(
          (col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_log_type"
          ) === lit(2)).or(
            is_not_null(col("log_dw_bid.log_type").cast(IntegerType))
              .and(col("log_dw_bid.log_type").cast(IntegerType) === lit(2))
          ),
          col("log_impbus_impressions.user_group_id").cast(IntegerType)
        ),
        col("log_dw_bid.user_group_id").cast(IntegerType)
      ).as("user_group_id"),
      lit(null).cast(IntegerType).as("camp_dp_id"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var").isin(8, 1, 3),
             lit(0)
        ),
        col("publisher_id").cast(IntegerType)
      ).as("media_buy_id"),
      col("in_f_create_agg_dw_impressions.media_buy_cost2_var")
        .as("media_buy_cost"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).as("brand_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.cleared_direct").cast(IntegerType),
        col("log_impbus_preempt.cleared_direct").cast(IntegerType)
      ).as("cleared_direct"),
      lit(null).cast(DoubleType).as("clear_fees"),
      when(col("in_f_create_agg_dw_impressions.imp_type_var") =!= lit(9),
           col("log_impbus_impressions.media_buy_rev_share_pct")
      ).otherwise(col("log_dw_bid.media_buy_rev_share_pct"))
        .as("media_buy_rev_share_pct"),
      col("in_f_create_agg_dw_impressions.revenue_value_var")
        .as("revenue_value"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              string_compare(col("log_dw_bid.pricing_type"), lit("--")) =!= lit(
                0
              )
            ),
          col("log_dw_bid.pricing_type")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_pricing_type")
        )
      ).as("pricing_type"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.can_convert").cast(IntegerType) =!= lit(0)),
        col("log_dw_bid.can_convert").cast(IntegerType)
      ).as("can_convert"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.is_placeholder_bid_var") === lit(
            1
          )).or(col("imp_type").cast(IntegerType) === lit(1))
            .or(col("imp_type").cast(IntegerType) === lit(3))
            .or(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(8)),
          lit(0)
        ),
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType)
      ).as("pub_rule_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.is_control").cast(IntegerType) =!= lit(0)),
          col("log_dw_bid.is_control").cast(IntegerType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0))
            .and(
              col(
                "in_f_create_agg_dw_impressions.virtual_log_dw_bid_is_control"
              ) =!= lit(0)
            ),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_is_control")
        )
      ).as("is_control"),
      when(col("log_dw_bid.control_pct") =!= lit(0),
           col("log_dw_bid.control_pct")
      ).as("control_pct"),
      when(col("log_dw_bid.control_creative_id").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.control_creative_id").cast(IntegerType)
      ).as("control_creative_id"),
      lit(null).cast(DoubleType).as("predicted_cpm"),
      coalesce(
        when((col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(
               1
             )).and(col("log_dw_bid.price") > lit(999)),
             lit(999)
        ),
        when((col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(
               1
             )).and(col("log_dw_bid.price") <= lit(999)),
             col("log_dw_bid.price")
        ),
        lit(0)
      ).as("actual_bid"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.content_category_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.content_category_id").cast(IntegerType)
      ).as("content_category_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.auction_service_fees_var")
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(0)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.auction_service_fees_var")
        ),
        when(
          f_is_non_cpm_payment_or_payment(
            col("in_f_create_agg_dw_impressions.imp_type_var"),
            col("in_f_create_agg_dw_impressions.payment_type_normalized_var"),
            col("in_f_create_agg_dw_impressions.revenue_type_normalized_var")
          ) === lit(1),
          lit(0)
        ).otherwise(
          col("in_f_create_agg_dw_impressions.auction_service_fees_var")
        )
      ).as("auction_service_fees"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.discrepancy_allowance_var")
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(0)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.discrepancy_allowance_var")
        ),
        when(
          f_is_non_cpm_payment_or_payment(
            col("in_f_create_agg_dw_impressions.imp_type_var"),
            col("in_f_create_agg_dw_impressions.payment_type_normalized_var"),
            col("in_f_create_agg_dw_impressions.revenue_type_normalized_var")
          ) === lit(1),
          lit(0)
        ).otherwise(
          col("in_f_create_agg_dw_impressions.discrepancy_allowance_var")
        )
      ).as("discrepancy_allowance"),
      lit(null).cast(DoubleType).as("forex_allowance"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.creative_overage_fees_var")
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(0)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.creative_overage_fees_var")
        ),
        when(
          f_is_non_cpm_payment_or_payment(
            col("in_f_create_agg_dw_impressions.imp_type_var"),
            col("in_f_create_agg_dw_impressions.payment_type_normalized_var"),
            col("in_f_create_agg_dw_impressions.revenue_type_normalized_var")
          ) === lit(1),
          lit(0)
        ).otherwise(
          col("in_f_create_agg_dw_impressions.creative_overage_fees_var")
        )
      ).as("creative_overage_fees"),
      coalesce(col("log_impbus_impressions.fold_position").cast(IntegerType),
               lit(0)
      ).as("fold_position"),
      when(col("log_impbus_impressions.external_inv_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.external_inv_id").cast(IntegerType)
      ).as("external_inv_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("log_dw_bid.cadence_modifier") =!= lit(0)),
          col("log_dw_bid.cadence_modifier")
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0))
            .and(
              col(
                "in_f_create_agg_dw_impressions.virtual_log_dw_bid_cadence_modifier"
              ) =!= lit(0)
            ),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_cadence_modifier"
          )
        )
      ).as("cadence_modifier"),
      col("in_f_create_agg_dw_impressions.imp_type_var")
        .cast(IntegerType)
        .as("imp_type"),
      coalesce(when(col(
                      "in_f_create_agg_dw_impressions._f_is_buy_side_var"
                    ) === lit(1),
                    col("log_dw_bid.advertiser_currency")
               ),
               lit("USD")
      ).as("advertiser_currency"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(is_not_null(col("log_dw_bid.advertiser_exchange_rate")))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(1.0d))
            .and(col("log_dw_bid.advertiser_exchange_rate") =!= lit(0.0d)),
          col("log_dw_bid.advertiser_exchange_rate")
        ),
        lit(1.0d)
      ).as("advertiser_exchange_rate"),
      col("log_impbus_impressions.ip_address").as("ip_address"),
      when(
        col("log_impbus_impressions.publisher_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.publisher_id").cast(IntegerType)
      ).as("publisher_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(1)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.auction_service_deduction_var")
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7))
            .and(
              col(
                "in_f_create_agg_dw_impressions.is_dw_normalized_var"
              ) === lit(0)
            )
            .and(
              col(
                "in_f_create_agg_dw_impressions.should_process_views_var"
              ) === lit(1)
            ),
          col("in_f_create_agg_dw_impressions.auction_service_deduction_var")
        ),
        when(
          f_is_non_cpm_payment_or_payment(
            col("in_f_create_agg_dw_impressions.imp_type_var"),
            col("in_f_create_agg_dw_impressions.payment_type_normalized_var"),
            col("in_f_create_agg_dw_impressions.revenue_type_normalized_var")
          ) === lit(1),
          lit(0)
        ).otherwise(
          col("in_f_create_agg_dw_impressions.auction_service_deduction_var")
        )
      ).as("auction_service_deduction"),
      coalesce(
        when(
          (col("imp_type").cast(IntegerType) === lit(1))
            .or(
              col("in_f_create_agg_dw_impressions.buyer_member_id_var") === lit(
                0
              )
            )
            .and(
              is_not_null(
                col("in_f_create_agg_dw_impressions.insertion_order_id_var")
              )
            ),
          lit(0)
        ),
        when(col(
               "in_f_create_agg_dw_impressions.insertion_order_id_var"
             ) =!= lit(0),
             col("in_f_create_agg_dw_impressions.insertion_order_id_var")
        )
      ).as("insertion_order_id"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_predict_type")
        )
      ).as("predict_type_rev"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.predict_type_goal").cast(IntegerType) =!= lit(0)
            ),
          col("log_dw_bid.predict_type_goal").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_predict_type_goal"
          )
        )
      ).as("predict_type_goal"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.predict_type").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.virtual_log_dw_bid_predict_type")
        )
      ).as("predict_type_cost"),
      col("in_f_create_agg_dw_impressions.booked_revenue_dollars_var")
        .as("booked_revenue_dollars"),
      col("in_f_create_agg_dw_impressions.booked_revenue_adv_curr_var")
        .as("booked_revenue_adv_curr"),
      when(col("in_f_create_agg_dw_impressions.commission_cpm_var") =!= lit(0),
           col("in_f_create_agg_dw_impressions.commission_cpm_var")
      ).as("commission_cpm"),
      when(col(
             "in_f_create_agg_dw_impressions.commission_revshare_var"
           ) =!= lit(0),
           col("in_f_create_agg_dw_impressions.commission_revshare_var")
      ).as("commission_revshare"),
      when(
        col("in_f_create_agg_dw_impressions.serving_fees_cpm_var") =!= lit(0),
        col("in_f_create_agg_dw_impressions.serving_fees_cpm_var")
      ).as("serving_fees_cpm"),
      when(col(
             "in_f_create_agg_dw_impressions.serving_fees_revshare_var"
           ) =!= lit(0),
           col("in_f_create_agg_dw_impressions.serving_fees_revshare_var")
      ).as("serving_fees_revshare"),
      when(col("log_impbus_impressions.user_tz_offset").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.user_tz_offset").cast(IntegerType)
      ).as("user_tz_offset"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.media_type").cast(IntegerType)),
        col("log_impbus_impressions.media_type").cast(IntegerType),
        col("log_impbus_preempt.media_type").cast(IntegerType)
      ).as("media_type"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).as("operating_system"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).as("browser"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).as("language"),
      col("log_impbus_impressions.seller_currency").as("publisher_currency"),
      coalesce(
        when(
          is_not_null(col("log_impbus_impressions.seller_exchange_rate")).and(
            col("log_impbus_impressions.seller_exchange_rate") =!= lit(1.0d)
          ),
          col("log_impbus_impressions.seller_exchange_rate")
        ),
        lit(1.0d)
      ).as("publisher_exchange_rate"),
      col("in_f_create_agg_dw_impressions.media_cost_dollars_cpm_var")
        .as("media_cost_dollars_cpm"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.is_placeholder_bid_var") === lit(
            1
          )).or(
            (col("imp_type").cast(IntegerType) === lit(1))
              .or(col("imp_type").cast(IntegerType) === lit(3))
              .or(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(8))
          ),
          lit(0)
        ),
        col("in_f_create_agg_dw_impressions.payment_type_normalized_var")
      ).as("payment_type"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.is_placeholder_bid_var") === lit(
            1
          )).or(col("imp_type").cast(IntegerType).isin(1,                  3))
            .or(col("in_f_create_agg_dw_impressions.imp_type_var").isin(2, 8)),
          lit(-1)
        ),
        col("in_f_create_agg_dw_impressions.revenue_type_normalized_var")
      ).as("revenue_type"),
      coalesce(
        when(col(
               "in_f_create_agg_dw_impressions.should_zero_seller_revenue_var"
             ).cast(BooleanType),
             lit(0)
        ),
        when(
          (col("in_f_create_agg_dw_impressions.v_transaction_event_pricing_var")
            .getField("buyer_charges")
            .getField("is_dw") === lit(1)).and(
            col("seller_member_id").cast(IntegerType) === col("buyer_member_id")
              .cast(IntegerType)
          ),
          col("in_f_create_agg_dw_impressions.seller_revenue_cpm_var")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col("in_f_create_agg_dw_impressions.seller_revenue_cpm_var")
        ),
        when(
          not(
            (col(
              "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var"
            ).getField("buyer_charges").getField("is_dw") === lit(1)).and(
              col("seller_member_id")
                .cast(IntegerType) === col("buyer_member_id").cast(IntegerType)
            )
          ),
          lit(0)
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(9),
             lit(0)
        )
      ).as("seller_revenue_cpm"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      when(string_compare(col("log_impbus_impressions.inv_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.inv_code")
      ).as("inv_code"),
      when(string_compare(col("log_impbus_impressions.application_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.application_id")
      ).as("application_id"),
      when(col("log_impbus_impressions.shadow_price") =!= lit(0.0d),
           col("log_impbus_impressions.shadow_price")
      ).as("shadow_price"),
      when(col("log_impbus_impressions.eap") =!= lit(0),
           col("log_impbus_impressions.eap")
      ).as("eap"),
      when(col("log_impbus_impressions.datacenter_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.datacenter_id").cast(IntegerType)
      ).as("datacenter_id"),
      when(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType)
      ).as("imp_blacklist_or_fraud"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_domains").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
      ).as("vp_expose_domains"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_categories").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_categories").cast(IntegerType)
      ).as("vp_expose_categories"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_pubs").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType)
      ).as("vp_expose_pubs"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_expose_tag").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)
      ).as("vp_expose_tag"),
      when(col("log_dw_bid.vp_expose_age").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_age").cast(IntegerType)
      ).as("vp_expose_age"),
      when(col("log_dw_bid.vp_expose_gender").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.vp_expose_gender").cast(IntegerType)
      ).as("vp_expose_gender"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).as("inventory_url_id"),
      when(
        col("log_impbus_impressions.audit_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.audit_type").cast(IntegerType)
      ).as("audit_type"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).as("truncate_ip"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).as("device_id"),
      when(
        col("log_impbus_impressions.carrier_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.carrier_id").cast(IntegerType)
      ).as("carrier_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.creative_audit_status").cast(IntegerType),
        col("log_impbus_preempt.creative_audit_status").cast(IntegerType)
      ).as("creative_audit_status"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.is_creative_hosted").cast(IntegerType),
        col("log_impbus_preempt.is_creative_hosted").cast(IntegerType)
      ).as("is_creative_hosted"),
      coalesce(
        when(col(
               "in_f_create_agg_dw_impressions.should_zero_seller_revenue_var"
             ).cast(BooleanType),
             lit(0)
        ),
        col("in_f_create_agg_dw_impressions.seller_deduction_var")
      ).as("seller_deduction"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).as("city"),
      when(string_compare(col("log_impbus_impressions.latitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.latitude")
      ).as("latitude"),
      when(string_compare(col("log_impbus_impressions.longitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.longitude")
      ).as("longitude"),
      when(string_compare(col("log_impbus_impressions.device_unique_id"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.device_unique_id")
      ).as("device_unique_id"),
      when(
        col("log_impbus_impressions.package_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.package_id").cast(IntegerType)
      ).as("package_id"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            string_compare(col("log_dw_bid.targeted_segments"),
                           lit("")
            ) =!= lit(0)
          ),
        col("log_dw_bid.targeted_segments")
      ).as("targeted_segments"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).as("supply_type"),
      when(
        col("log_impbus_impressions.is_toolbar").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_toolbar").cast(IntegerType)
      ).as("is_toolbar"),
      col("in_f_create_agg_dw_impressions.deal_id_var").as("deal_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_bitmap").cast(LongType),
        col("log_impbus_preempt.vp_bitmap").cast(LongType)
      ).as("vp_bitmap"),
      col("in_f_create_agg_dw_impressions.view_detection_enabled_var")
        .as("view_detection_enabled"),
      col("in_f_create_agg_dw_impressions.view_result_var").as("view_result"),
      when(col("log_impbus_impressions.ozone_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.ozone_id").cast(IntegerType)
      ).as("ozone_id"),
      when(col("log_impbus_impressions.is_performance").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.is_performance").cast(IntegerType)
      ).as("is_performance"),
      when(string_compare(col("log_impbus_impressions.sdk_version"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.sdk_version")
      ).as("sdk_version"),
      when(
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.inventory_session_frequency").cast(
          IntegerType
        )
      ).as("inventory_session_frequency"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).as("device_type"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).as("dma"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal"),
      col("in_f_create_agg_dw_impressions.viewdef_definition_id_var")
        .as("viewdef_definition_id"),
      col("in_f_create_agg_dw_impressions.viewdef_viewable_var")
        .as("viewdef_viewable"),
      col("in_f_create_agg_dw_impressions.view_measurable_var")
        .as("view_measurable"),
      col("in_f_create_agg_dw_impressions.viewable_var").as("viewable"),
      when(col("log_impbus_impressions.is_secure").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.is_secure").cast(IntegerType)
      ).as("is_secure"),
      col("in_f_create_agg_dw_impressions.view_non_measurable_reason_var")
        .as("view_non_measurable_reason"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.buyer_trx_event_id_var") === lit(
            1
          )).and(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6)),
          col("in_f_create_agg_dw_impressions.data_costs_deal_var")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("in_f_create_agg_dw_impressions.data_costs_var")
        )
      ).as("data_costs"),
      when(col("log_impbus_impressions.bidder_instance_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.bidder_instance_id").cast(IntegerType)
      ).as("bidder_instance_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.campaign_group_freq")
                .cast(IntegerType) =!= lit(-2)
            ),
          col("log_dw_bid.campaign_group_freq").cast(IntegerType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0))
            .and(
              col(
                "in_f_create_agg_dw_impressions.virtual_log_dw_bid_campaign_group_freq"
              ) =!= lit(-2)
            ),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_campaign_group_freq"
          )
        )
      ).as("campaign_group_freq"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.campaign_group_rec").cast(IntegerType) =!= lit(0)
            ),
          col("log_dw_bid.campaign_group_rec").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_campaign_group_rec"
          )
        )
      ).as("campaign_group_rec"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.insertion_order_freq")
                .cast(IntegerType) =!= lit(-2)
            ),
          col("log_dw_bid.insertion_order_freq").cast(IntegerType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0))
            .and(
              col(
                "in_f_create_agg_dw_impressions.virtual_log_dw_bid_insertion_order_freq"
              ) =!= lit(-2)
            ),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_insertion_order_freq"
          )
        )
      ).as("insertion_order_freq"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.insertion_order_rec").cast(IntegerType) =!= lit(0)
            ),
          col("log_dw_bid.insertion_order_rec").cast(IntegerType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_insertion_order_rec"
          )
        )
      ).as("insertion_order_rec"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            string_compare(col("log_dw_bid.buyer_gender"), lit("u")) =!= lit(0)
          ),
        col("log_dw_bid.buyer_gender")
      ).as("buyer_gender"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(is_not_null(col("log_dw_bid.buyer_age").cast(IntegerType)))
            .and(col("log_dw_bid.buyer_age").cast(IntegerType) =!= lit(-1)),
          col("log_dw_bid.buyer_age").cast(IntegerType)
        ),
        lit(0)
      ).as("buyer_age"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(is_not_null(col("log_dw_bid.targeted_segment_list"))),
        col("log_dw_bid.targeted_segment_list")
      ).as("targeted_segment_list"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.custom_model_id").cast(IntegerType) =!= lit(0)),
        col("log_dw_bid.custom_model_id").cast(IntegerType)
      ).as("custom_model_id"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            col("log_dw_bid.custom_model_last_modified")
              .cast(LongType) =!= lit(0)
          ),
        col("log_dw_bid.custom_model_last_modified").cast(LongType)
      ).as("custom_model_last_modified"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              string_compare(col("log_dw_bid.custom_model_output_code"),
                             lit("")
              ) =!= lit(0)
            ),
          col("log_dw_bid.custom_model_output_code")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_custom_model_output_code"
          )
        )
      ).as("custom_model_output_code"),
      when(string_compare(col("log_impbus_impressions.external_uid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_uid")
      ).as("external_uid"),
      when(string_compare(col("log_impbus_impressions.request_uuid"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.request_uuid")
      ).as("request_uuid"),
      when(
        col("log_impbus_impressions.mobile_app_instance_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.mobile_app_instance_id").cast(IntegerType)
      ).as("mobile_app_instance_id"),
      when(string_compare(col("log_impbus_impressions.traffic_source_code"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.traffic_source_code")
      ).as("traffic_source_code"),
      when(string_compare(col("log_impbus_impressions.external_request_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.external_request_id")
      ).as("external_request_id"),
      when(string_compare(col("log_impbus_impressions.stitch_group_id"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.stitch_group_id")
      ).as("stitch_group_id"),
      col("in_f_create_agg_dw_impressions.deal_type_var").as("deal_type"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_floor_id").cast(IntegerType),
        col("log_impbus_preempt.ym_floor_id").cast(IntegerType)
      ).as("ym_floor_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_bias_id").cast(IntegerType),
        col("log_impbus_preempt.ym_bias_id").cast(IntegerType)
      ).as("ym_bias_id"),
      when(col("log_dw_bid.bid_priority").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.bid_priority").cast(IntegerType)
      ).as("bid_priority"),
      buyer_charges(context).as("buyer_charges"),
      seller_charges(context).as("seller_charges"),
      when(col("log_dw_bid.explore_disposition").cast(IntegerType) =!= lit(0),
           col("log_dw_bid.explore_disposition").cast(IntegerType)
      ).as("explore_disposition"),
      when(col("log_impbus_impressions.device_make_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.device_make_id").cast(IntegerType)
      ).as("device_make_id"),
      when(
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        ) =!= lit(1),
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        )
      ).as("operating_system_family_id"),
      col("log_impbus_impressions.tag_sizes").as("tag_sizes"),
      lit(null)
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
      coalesce(col("log_impbus_impressions_pricing.rate_card_media_type").cast(
                 IntegerType
               ),
               lit(0)
      ).as("pricing_media_type"),
      col("in_f_create_agg_dw_impressions.buyer_trx_event_id_var")
        .as("buyer_trx_event_id"),
      col("in_f_create_agg_dw_impressions.seller_trx_event_id_var")
        .as("seller_trx_event_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(0))
            .and(
              col(
                "in_f_create_agg_dw_impressions.virtual_log_dw_bid_revenue_auction_event_type"
              ) =!= lit(0)
            ),
          col(
            "in_f_create_agg_dw_impressions.virtual_log_dw_bid_revenue_auction_event_type"
          )
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              col("log_dw_bid.revenue_auction_event_type")
                .cast(IntegerType) =!= lit(0)
            ),
          col("log_dw_bid.revenue_auction_event_type").cast(IntegerType)
        )
      ).as("revenue_auction_event_type"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(BooleanType)
      ).as("is_prebid"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6),
             col("in_f_create_agg_dw_impressions.has_seller_transacted_var")
               .cast(BooleanType)
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var").isin(7, 11),
             col("in_f_create_agg_dw_impressions.has_buyer_transacted_var")
               .cast(BooleanType)
        ),
        lit(1).cast(BooleanType)
      ).as("is_unit_of_trx"),
      col("in_f_create_agg_dw_impressions.imps_for_budget_caps_pacing_var")
        .as("imps_for_budget_caps_pacing"),
      col("log_impbus_impressions.date_time")
        .cast(LongType)
        .as("auction_timestamp"),
      when(
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          ByteType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.two_phase_reduction_applied").cast(
          BooleanType
        )
      ).as("two_phase_reduction_applied"),
      when(col("log_impbus_impressions.region_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.region_id").cast(IntegerType)
      ).as("region_id"),
      when(col("log_impbus_impressions.media_company_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.media_company_id").cast(IntegerType)
      ).as("media_company_id"),
      when(
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.trade_agreement_id").cast(
          IntegerType
        )
      ).as("trade_agreement_id"),
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
      coalesce(
        when(
          is_not_null(col("in_f_create_agg_dw_impressions.netflix_ppid_var"))
            .cast(BooleanType),
          f_string_to_anon_user_info(
            col("in_f_create_agg_dw_impressions.netflix_ppid_var")
          )
        ),
        col("log_impbus_impressions.anonymized_user_info")
      ).as("anonymized_user_info"),
      when(string_compare(col("log_impbus_impressions.gdpr_consent_cookie"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.gdpr_consent_cookie")
      ).as("gdpr_consent_cookie"),
      col("additional_clearing_events"),
      col("log_impbus_impressions.fx_rate_snapshot_id")
        .cast(IntegerType)
        .as("fx_rate_snapshot_id"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.crossdevice_group_anon")
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6),
             col("log_dw_bid_deal.crossdevice_group_anon")
        )
      ).as("crossdevice_group_anon"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions.buyer_trx_event_id_var") === lit(
            1
          )).and(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6)),
          col("log_dw_bid_deal.crossdevice_graph_cost")
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(col("in_f_create_agg_dw_impressions.imp_type_var") =!= lit(6)),
          col("log_dw_bid.crossdevice_graph_cost")
        )
      ).as("crossdevice_graph_cost"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            col("log_dw_bid.revenue_event_type_id").cast(IntegerType) =!= lit(0)
          ),
        col("log_dw_bid.revenue_event_type_id").cast(IntegerType)
      ).as("revenue_event_type_id"),
      col("in_f_create_agg_dw_impressions.buyer_trx_event_type_id_var")
        .as("buyer_trx_event_type_id"),
      col("in_f_create_agg_dw_impressions.seller_trx_event_type_id_var")
        .as("seller_trx_event_type_id"),
      coalesce(col("log_impbus_preempt.external_creative_id"),
               when(is_not_null(col("log_impbus_preempt")).cast(BooleanType),
                    lit("---")
               )
      ).as("external_creative_id"),
      coalesce(
        when(is_not_null(col("log_dw_bid_deal")).and(
               col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6)
             ),
             col("log_dw_bid_deal.targeted_segment_details")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.targeted_segment_details")
        )
      ).as("targeted_segment_details"),
      coalesce(
        col("log_impbus_preempt.seat_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")).cast(BooleanType), lit(0)),
        lit(0)
      ).as("bidder_seat_id"),
      when(col("log_impbus_impressions.is_whiteops_scanned").cast(
             ByteType
           ) =!= lit(0),
           col("log_impbus_impressions.is_whiteops_scanned").cast(BooleanType)
      ).as("is_whiteops_scanned"),
      lit(null).cast(StringType).as("default_referrer_url"),
      when(col("in_f_create_agg_dw_impressions.is_curated_var") === lit(1),
           lit(1).cast(BooleanType)
      ).as("is_curated"),
      col("in_f_create_agg_dw_impressions.curator_member_id_var")
        .as("curator_member_id"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_partner_fees_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_partner_fees_microcents"
          ).cast(LongType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_partner_fees_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_bid.revenue_info.total_partner_fees_microcents").cast(
            LongType
          )
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          lit(0)
        )
      ).as("total_partner_fees_microcents"),
      lit(null).cast(DoubleType).as("net_buyer_spend"),
      when(col("log_impbus_preempt.is_prebid_server").cast(ByteType) =!= lit(0),
           col("log_impbus_preempt.is_prebid_server").cast(BooleanType)
      ).as("is_prebid_server"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            col("log_dw_bid.cold_start_price_type")
              .cast(IntegerType) =!= lit(-1)
          ),
        col("log_dw_bid.cold_start_price_type").cast(IntegerType)
      ).as("cold_start_price_type"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.discovery_state").cast(IntegerType) =!= lit(-1)),
        col("log_dw_bid.discovery_state").cast(IntegerType)
      ).as("discovery_state"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.insertion_order_budget_interval_id").cast(IntegerType)
        ),
        col("in_f_create_agg_dw_impressions.billing_period_id_var"),
        lit(0)
      ).as("billing_period_id"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.campaign_group_budget_interval_id").cast(IntegerType)
        ),
        col("in_f_create_agg_dw_impressions.flight_id_var"),
        lit(0)
      ).as("flight_id"),
      col("in_f_create_agg_dw_impressions.split_id_var").as("split_id"),
      when(
        (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(7)).and(
          col("in_f_create_agg_dw_impressions.v_transaction_event_pricing_var")
            .getField("buyer_transacted") === lit(1)
        ),
        col("in_f_create_agg_dw_impressions.v_transaction_event_pricing_var")
          .getField("net_payment_value_microcents")
          .cast(DoubleType) / lit(100000.0d)
      ).as("net_media_cost_dollars_cpm"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_data_costs_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_data_costs_microcents"
          ).cast(LongType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_data_costs_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_bid.revenue_info.total_data_costs_microcents").cast(
            LongType
          )
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          lit(0)
        )
      ).as("total_data_costs_microcents"),
      coalesce(
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_view.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            )
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_view.revenue_info.total_profit_microcents")
            .cast(LongType) + col(
            "log_dw_bid.revenue_info.total_profit_microcents"
          ).cast(LongType)
        ),
        when(
          (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
            .and(
              is_not_null(
                col("log_dw_bid.revenue_info.total_profit_microcents")
                  .cast(LongType)
              )
            ),
          col("log_dw_bid.revenue_info.total_profit_microcents").cast(LongType)
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          lit(0)
        )
      ).as("total_profit_microcents"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(
            col("log_dw_bid.targeted_crossdevice_graph_id")
              .cast(IntegerType) =!= lit(0)
          ),
        col("log_dw_bid.targeted_crossdevice_graph_id").cast(IntegerType)
      ).as("targeted_crossdevice_graph_id"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.discovery_prediction") =!= lit(0.0d)),
        col("log_dw_bid.discovery_prediction")
      ).as("discovery_prediction"),
      col("in_f_create_agg_dw_impressions.campaign_group_type_id_var")
        .as("campaign_group_type_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).as("hb_source"),
      col("in_f_create_agg_dw_impressions.f_preempt_over_impression_94298_var")
        .as("external_campaign_id"),
      when(col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
           col("log_dw_bid.excluded_targeted_segment_details")
      ).as("excluded_targeted_segment_details"),
      lit(null).cast(StringType).as("trust_id"),
      when(
        (col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1))
          .and(col("log_dw_bid.predicted_kpi_event_rate") =!= lit(0.0d)),
        col("log_dw_bid.predicted_kpi_event_rate")
      ).as("predicted_kpi_event_rate"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.has_crossdevice_reach_extension").cast(BooleanType)
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6),
             col("log_dw_bid_deal.has_crossdevice_reach_extension").cast(
               BooleanType
             )
        )
      ).as("has_crossdevice_reach_extension"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.crossdevice_graph_membership")
        ),
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6),
             col("log_dw_bid_deal.crossdevice_graph_membership")
        )
      ).as("crossdevice_graph_membership"),
      when(
        col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
        coalesce(col(
                   "log_dw_bid.revenue_info.total_segment_data_costs_microcents"
                 ).cast(LongType),
                 lit(0)
        ) + coalesce(
          col("log_dw_view.revenue_info.total_segment_data_costs_microcents")
            .cast(LongType),
          lit(0)
        )
      ).otherwise(lit(0)).as("total_segment_data_costs_microcents"),
      when(
        col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
        coalesce(col("log_dw_bid.revenue_info.total_feature_costs_microcents")
                   .cast(LongType),
                 lit(0)
        ) + coalesce(col(
                       "log_dw_view.revenue_info.total_feature_costs_microcents"
                     ).cast(LongType),
                     lit(0)
        )
      ).otherwise(lit(0)).as("total_feature_costs_microcents"),
      when(
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions_pricing.counterparty_ruleset_type").cast(
          IntegerType
        )
      ).as("counterparty_ruleset_type"),
      coalesce(col("log_impbus_preempt.log_product_ads"),
               col("log_impbus_impressions.log_product_ads")
      ).as("log_product_ads"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var").isin(7, 5),
             coalesce(col("log_dw_bid.line_item_currency"),             lit("---"))
        ),
        lit("---")
      ).as("buyer_line_item_currency"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6),
             coalesce(col("log_dw_bid_deal.line_item_currency"), lit("---"))
        ),
        lit("---")
      ).as("deal_line_item_currency"),
      coalesce(when(col(
                      "in_f_create_agg_dw_impressions._f_is_buy_side_var"
                    ) === lit(1),
                    col("log_dw_bid.measurement_fee_cpm_usd")
               ),
               lit(0)
      ).as("measurement_fee_usd"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.measurement_provider_member_id").cast(IntegerType)
        ),
        lit(0)
      ).as("measurement_provider_member_id"),
      when(col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
           col("log_dw_bid.offline_attribution_provider_member_id").cast(
             IntegerType
           )
      ).as("offline_attribution_provider_member_id"),
      when(col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
           col("log_dw_bid.offline_attribution_cost_usd_cpm")
      ).as("offline_attribution_cost_usd_cpm"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).as("pred_info"),
      col("log_impbus_impressions.imp_rejecter_do_auction")
        .cast(BooleanType)
        .as("imp_rejecter_do_auction"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_impbus_impressions.is_imp_rejecter_applied")
        .cast(BooleanType)
        .as("imp_rejecter_applied"),
      lit(null).cast(FloatType).as("ip_derived_latitude"),
      lit(null).cast(FloatType).as("ip_derived_longitude"),
      col("log_impbus_impressions.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("log_impbus_impressions.postal_code_ext_id")
        .cast(IntegerType)
        .as("postal_code_ext_id"),
      coalesce(when(col(
                      "in_f_create_agg_dw_impressions.buyer_trx_event_id_var"
                    ) === lit(2),
                    col("log_dw_view.ecpm_conversion_rate")
               ),
               lit(1.0d)
      ).as("ecpm_conversion_rate"),
      when(col(
             "in_f_create_agg_dw_impressions.sup_ip_range_lookup_count_var"
           ) > lit(0),
           lit(1).cast(BooleanType)
      ).otherwise(lit(0).cast(BooleanType)).as("is_residential_ip"),
      col("log_impbus_impressions.hashed_ip").as("hashed_ip"),
      coalesce(
        when(
          is_not_null(col("log_dw_bid_deal"))
            .and(col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6)),
          col("log_dw_bid_deal.targeted_segment_details_by_id_type")
        ),
        when(
          col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
          col("log_dw_bid.targeted_segment_details_by_id_type")
        )
      ).as("targeted_segment_details_by_id_type"),
      when(col("in_f_create_agg_dw_impressions._f_is_buy_side_var") === lit(1),
           col("log_dw_bid.offline_attribution")
      ).as("offline_attribution"),
      col("log_dw_bid.frequency_cap_type_internal")
        .cast(IntegerType)
        .as("frequency_cap_type_internal"),
      col("log_dw_bid.modeled_cap_did_override_line_item_daily_cap")
        .cast(BooleanType)
        .as("modeled_cap_did_override_line_item_daily_cap"),
      col("log_dw_bid.modeled_cap_user_sample_rate")
        .as("modeled_cap_user_sample_rate"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(
          col("log_impbus_impressions_pricing_dup.estimated_audience_imps"),
          lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("estimated_audience_imps"),
      when(
        coalesce(col("log_impbus_impressions.device_type").cast(IntegerType),
                 lit(99)
        ) === lit(8),
        coalesce(col("log_impbus_impressions_pricing_dup.audience_imps"),
                 lit(1.0d)
        )
      ).otherwise(lit(1.0d)).as("audience_imps"),
      col("log_dw_bid.district_postal_code_lists")
        .as("district_postal_code_lists"),
      coalesce(
        when(col("in_f_create_agg_dw_impressions.imp_type_var").isin(4, 7, 5),
             coalesce(col("log_dw_bid.bidding_host_id").cast(IntegerType),
                      lit(0)
             )
        ),
        when(
          (col("in_f_create_agg_dw_impressions.imp_type_var") === lit(6))
            .and(is_not_null(col("log_dw_bid_deal"))),
          coalesce(col("log_dw_bid_deal.bidding_host_id").cast(IntegerType),
                   lit(0)
          )
        )
      ).as("bidding_host_id"),
      lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
      lit(null).cast(LongType).as("seller_dpvp_bitmap"),
      col("log_impbus_impressions.browser_code_id")
        .cast(IntegerType)
        .as("browser_code_id"),
      col("log_impbus_impressions.is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      coalesce(col("log_dw_bid.feature_tests_bitmap").cast(IntegerType), lit(0))
        .as("feature_tests_bitmap"),
      coalesce(col("log_impbus_impressions.private_auction_eligible").cast(
                 BooleanType
               ),
               lit(0).cast(BooleanType)
      ).as("private_auction_eligible"),
      coalesce(
        col("log_impbus_impressions.chrome_traffic_label").cast(IntegerType),
        lit(0)
      ).as("chrome_traffic_label"),
      coalesce(
        col("log_impbus_impressions.is_private_auction").cast(BooleanType),
        lit(0).cast(BooleanType)
      ).as("is_private_auction"),
      col("in_f_create_agg_dw_impressions.f_preempt_over_impression_95337_var")
        .as("creative_media_subtype_id"),
      col("log_impbus_impressions.allowed_media_types").as(
        "allowed_media_types"
      )
    )

  def buyer_charges(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.rate_card_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.rate_card_id").cast(
          IntegerType
        )
      ).as("rate_card_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.member_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.member_id").cast(
          IntegerType
        )
      ).as("member_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.is_dw"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(ByteType)
      ).as("is_dw"),
      col("in_f_create_agg_dw_impressions.buyer_charges_pricing_terms_var")
        .as("pricing_terms"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.fx_margin_rate_id")
          .cast(IntegerType)
      ).as("fx_margin_rate_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.marketplace_owner_id")
          .cast(IntegerType)
      ).as("marketplace_owner_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType)
      ).as("virtual_marketplace_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.buyer_charges.amino_enabled"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.amino_enabled"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.amino_enabled"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.buyer_charges.amino_enabled").cast(
          ByteType
        )
      ).as("amino_enabled")
    )
  }

  def seller_charges(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.rate_card_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.rate_card_id").cast(
          IntegerType
        )
      ).as("rate_card_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.member_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.member_id").cast(
          IntegerType
        )
      ).as("member_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.is_dw"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
          ByteType
        )
      ).as("is_dw"),
      coalesce(
        when(
          col("in_f_create_agg_dw_impressions.should_zero_seller_revenue_var")
            .cast(BooleanType),
          f_drop_is_deduction_pricing_terms(
            col(
              "in_f_create_agg_dw_impressions.seller_charges_pricing_terms_var"
            )
          )
        ),
        col("in_f_create_agg_dw_impressions.seller_charges_pricing_terms_var")
      ).as("pricing_terms"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.fx_margin_rate_id")
          .cast(IntegerType)
      ).as("fx_margin_rate_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType)
      ).as("marketplace_owner_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType)
      ).as("virtual_marketplace_id"),
      coalesce(
        col(
          "in_f_create_agg_dw_impressions.v_transaction_event_pricing_var.seller_charges.amino_enabled"
        ).cast(IntegerType),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.amino_enabled"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.amino_enabled"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.seller_charges.amino_enabled").cast(
          ByteType
        )
      ).as("amino_enabled")
    )
  }

}
