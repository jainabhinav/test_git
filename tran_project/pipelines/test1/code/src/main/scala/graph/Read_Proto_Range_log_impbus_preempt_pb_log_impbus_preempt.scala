package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_log_impbus_preempt_pb_log_impbus_preempt {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load("dbfs:/FileStore/data_engg/msbing/transaction_graph/logbus_preempt")

}
