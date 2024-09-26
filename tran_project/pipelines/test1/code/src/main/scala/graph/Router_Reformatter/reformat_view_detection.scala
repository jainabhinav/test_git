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

object reformat_view_detection {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      f_viewable(
        f_view_measurable(
          f_view_detection_enabled(
            col("log_impbus_impressions.view_detection_enabled").cast(
              IntegerType
            ),
            col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
          ).cast(IntegerType),
          col("log_impbus_view.view_result").cast(IntegerType)
        ).cast(IntegerType),
        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType).as("abc"),
      f_view_measurable(
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ).cast(IntegerType),
        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType).as("test1"),
      col("log_impbus_view.view_result").cast(IntegerType).as("test2"),
      f_view_detection_enabled(
        col("log_impbus_impressions.view_detection_enabled").cast(IntegerType),
        col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
      ).cast(IntegerType).as("test3")
    )

}
