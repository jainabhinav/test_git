package graph.LookupToJoin50

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.LookupToJoin50.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_dw_impressions_PrevExpressionj0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.log_impbus_preempt.curated_deal_id").cast(
              IntegerType
            ) === col("in1.id"),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.log_impbus_preempt.deal_id").cast(IntegerType) === col(
              "in2.id"
            ),
            "left_outer"
      )
      .join(in3.as("in3"),
            col("in0.campaign_id").cast(IntegerType) === col("in3.campaign_id"),
            "left_outer"
      )
      .select(
        when(is_not_null(col("in1.id")),
             struct(col("in1.id").as("id"),
                    col("in1.member_id").as("member_id"),
                    col("in1.deal_type_id").as("deal_type_id")
             )
        ).as("_sup_common_deal_LOOKUP1"),
        when(is_not_null(col("in2.id")),
             struct(col("in2.id").as("id"),
                    col("in2.member_id").as("member_id"),
                    col("in2.deal_type_id").as("deal_type_id")
             )
        ).as("_sup_common_deal_LOOKUP"),
        when(
          is_not_null(col("in3.campaign_id")),
          struct(
            col("in3.campaign_id").as("campaign_id"),
            col("in3.campaign_type_id").as("campaign_type_id"),
            col("in3.campaign_group_id").as("campaign_group_id"),
            col("in3.campaign_group_type_id").as("campaign_group_type_id")
          )
        ).as("_sup_bidder_campaign_LOOKUP"),
        when(
          is_not_null(
            re_get_match(
              col("log_impbus_impressions.ip_address").cast(StringType),
              lit("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")
            )
          ),
          concat(
            element_at(split(col("log_impbus_impressions.ip_address"), "\\."),
                       1
            ),
            string_lpad(
              element_at(split(col("log_impbus_impressions.ip_address"), "\\."),
                         2
              ),
              3,
              "0"
            ).cast(StringType),
            string_lpad(
              element_at(split(col("log_impbus_impressions.ip_address"), "\\."),
                         3
              ),
              3,
              "0"
            ).cast(StringType)
          ).cast(IntegerType)
        ).otherwise(lit(0)).as("left_ip_address"),
        col("in0.*")
      )

}
