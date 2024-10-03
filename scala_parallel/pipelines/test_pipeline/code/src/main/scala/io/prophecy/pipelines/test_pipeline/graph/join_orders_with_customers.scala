package io.prophecy.pipelines.test_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.functions.PipelineInitCode._
import io.prophecy.pipelines.test_pipeline.functions.UDFs._
import io.prophecy.pipelines.test_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object join_orders_with_customers {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    context.instrument(
      in0
        .as("in0")
        .join(in1.as("in1"),
              col("in0.customer_id") === col("in1.customer_id"),
              "inner"
        )
        .select(
          col("in0.order_id").as("order_id"),
          col("in0.customer_id").as("customer_id"),
          col("in0.amount").as("amount"),
          col("in0.order_date").as("order_date"),
          col("in1.account_open_date").as("account_open_date"),
          col("in1.first_name").as("first_name"),
          col("in1.last_name").as("last_name")
        )
    )

}
