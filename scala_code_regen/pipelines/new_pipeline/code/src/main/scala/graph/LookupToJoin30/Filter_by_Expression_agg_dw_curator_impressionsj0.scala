package graph.LookupToJoin30

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.LookupToJoin30.config.Context
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
        col("in0.*")
      )

}
