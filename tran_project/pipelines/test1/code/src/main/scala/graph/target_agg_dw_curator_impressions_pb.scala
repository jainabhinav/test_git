package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object target_agg_dw_curator_impressions_pb {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load(
        "dbfs:/FileStore/data_engg/msbing/transaction_graph/prophecy_output/target_agg_dw_curator_impressions_pb"
      )

}
