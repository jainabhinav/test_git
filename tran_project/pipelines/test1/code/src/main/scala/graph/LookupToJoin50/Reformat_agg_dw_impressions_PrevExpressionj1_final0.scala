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

object Reformat_agg_dw_impressions_PrevExpressionj1_final0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("row_id"))
      .agg(
        size(collect_list(col("_lookup_result")))
          .as("_sup_ip_range_LOOKUP_COUNT"),
        List() ++ in.columns.toList
          .diff(List("_sup_ip_range_LOOKUP_COUNT", "row_id"))
          .map(x => first(col(x)).as(x)): _*
      )

}
