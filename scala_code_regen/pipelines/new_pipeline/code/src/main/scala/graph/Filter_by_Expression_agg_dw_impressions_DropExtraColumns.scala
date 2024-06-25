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

object Filter_by_Expression_agg_dw_impressions_DropExtraColumns {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.drop("_member_id_by_advertiser_id_LOOKUP")
      .drop("_advertiser_id_by_campaign_group_id_LOOKUP")
      .drop("_member_id_by_publisher_id_LOOKUP")

}
