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

object Reformat_log_dw_bid_PrevExpression {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn("in_f_convert_log_dw_imptracker_to_log_dw_bid",
                  f_convert_log_dw_imptracker_to_log_dw_bid()
    )

}
