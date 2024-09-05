package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_aggDwClicksPb_DropExtraColumns {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.drop("ll1_payment_type")
      .drop("ll1_revenue_type")
      .drop("ll1_buyer_charges")
      .drop("ll1_seller_charges")
      .drop("ll1_publisher_exchange_rate")
      .drop("ll1_buyer_member_id")
      .drop("ll2_auction_event_pricing")
      .drop("ll1_imp_type")
      .drop("ll1_data_costs")
      .drop("ll1_commission_revshare")
      .drop("ll0_revenue_info")
      .drop("ll1_serving_fees_revshare")
      .drop("ll1_media_buy_rev_share_pct")
      .drop("_lkp_sales_tax_rate_LOOKUP")

}
