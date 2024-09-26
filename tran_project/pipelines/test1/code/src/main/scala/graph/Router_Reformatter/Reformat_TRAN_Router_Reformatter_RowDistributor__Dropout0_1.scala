package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.drop("_advertiser_id_by_campaign_group_id_LOOKUP")
      .drop("_member_id_by_advertiser_id_LOOKUP")
      .drop("_member_id_by_publisher_id_LOOKUP")
      .drop("r_is_quarantined")
      .drop("r_is_dw")
      .drop("r_is_curated")
      .drop("r_is_transacted")

}
