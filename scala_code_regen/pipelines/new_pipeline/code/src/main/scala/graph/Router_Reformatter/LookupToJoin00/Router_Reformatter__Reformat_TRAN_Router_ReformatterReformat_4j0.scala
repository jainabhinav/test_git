package graph.Router_Reformatter.LookupToJoin00

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.LookupToJoin00.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame,
    in6:     DataFrame,
    in7:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.tag_id").cast(IntegerType).cast(IntegerType) === col(
              "in1.tag_id"
            ),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.campaign_id").cast(IntegerType).cast(IntegerType) === col(
              "in2.campaign_id"
            ),
            "left_outer"
      )
      .join(in3.as("in3"),
            col("in0.publisher_id").cast(IntegerType).cast(IntegerType) === col(
              "in3.publisher_id"
            ),
            "left_outer"
      )
      .join(
        in4.as("in4"),
        col("in0.advertiser_id").cast(IntegerType).cast(IntegerType) === col(
          "in4.advertiser_id"
        ),
        "left_outer"
      )
      .join(in5.as("in5"),
            col("in0.insertion_order_id")
              .cast(IntegerType)
              .cast(IntegerType) === col("in5.insertion_order_id"),
            "left_outer"
      )
      .join(in6.as("in6"),
            col("in0.campaign_group_id")
              .cast(IntegerType)
              .cast(IntegerType) === col("in6.campaign_group_id"),
            "left_outer"
      )
      .join(in7.as("in7"),
            col("in0.site_id").cast(IntegerType).cast(IntegerType) === col(
              "in7.site_id"
            ),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.tag_id")),
          struct(
            col("in1.seller_member_id").as("seller_member_id"),
            col("in1.publisher_id").as("publisher_id"),
            col("in1.site_id").as("site_id"),
            col("in1.tag_id").as("tag_id")
          )
        ).as("_publisher_id_by_tag_id_LOOKUP"),
        when(
          is_not_null(col("in2.campaign_id")),
          struct(
            col("in2.buyer_member_id").as("buyer_member_id"),
            col("in2.advertiser_id").as("advertiser_id"),
            col("in2.campaign_group_id").as("campaign_group_id"),
            col("in2.campaign_id").as("campaign_id")
          )
        ).as("_advertiser_id_by_campaign_id_LOOKUP"),
        when(
          is_not_null(col("in3.publisher_id")),
          struct(
            col("in3.seller_member_id").as("seller_member_id"),
            col("in3.publisher_id").as("publisher_id"),
            col("in3.site_id").as("site_id"),
            col("in3.tag_id").as("tag_id")
          )
        ).as("_member_id_by_publisher_id_LOOKUP"),
        when(
          is_not_null(col("in4.advertiser_id")),
          struct(
            col("in4.buyer_member_id").as("buyer_member_id"),
            col("in4.advertiser_id").as("advertiser_id"),
            col("in4.campaign_group_id").as("campaign_group_id"),
            col("in4.campaign_id").as("campaign_id")
          )
        ).as("_member_id_by_advertiser_id_LOOKUP"),
        when(
          is_not_null(col("in5.insertion_order_id")),
          struct(col("in5.buyer_member_id").as("buyer_member_id"),
                 col("in5.advertiser_id").as("advertiser_id"),
                 col("in5.insertion_order_id").as("insertion_order_id")
          )
        ).as("_advertiser_id_by_insertion_order_id_LOOKUP"),
        when(
          is_not_null(col("in6.campaign_group_id")),
          struct(
            col("in6.buyer_member_id").as("buyer_member_id"),
            col("in6.advertiser_id").as("advertiser_id"),
            col("in6.campaign_group_id").as("campaign_group_id"),
            col("in6.campaign_id").as("campaign_id")
          )
        ).as("_advertiser_id_by_campaign_group_id_LOOKUP"),
        when(
          is_not_null(col("in7.site_id")),
          struct(
            col("in7.seller_member_id").as("seller_member_id"),
            col("in7.publisher_id").as("publisher_id"),
            col("in7.site_id").as("site_id"),
            col("in7.tag_id").as("tag_id")
          )
        ).as("_publisher_id_by_site_id_LOOKUP"),
        col("in0.*")
      )

}
