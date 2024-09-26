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

object Reformat_agg_dw_impressions_PrevExpressionj1_join0 {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(
        right.as("right"),
        (col("left.left_ip_address") >= col("right.start_ip_number"))
          .and(col("left.left_ip_address") < col("right.end_ip_number")),
        "left_outer"
      )
      .select(
        when(
          is_not_null(col("right.start_ip_number"))
            .and(is_not_null(col("right.end_ip_number"))),
          struct(
            col("right.index").as("index"),
            col("right.start_ip").as("start_ip"),
            col("right.end_ip").as("end_ip"),
            col("right.name").as("name"),
            col("right.ip_feature").as("ip_feature"),
            col("right.start_ip_number").as("start_ip_number"),
            col("right.end_ip_number").as("end_ip_number")
          )
        ).as("_lookup_result"),
        col("left.*")
      )

}
