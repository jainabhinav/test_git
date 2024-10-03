package io.prophecy.pipelines.test_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object orders {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("order_id",       StringType, true),
            StructField("customer_id",    StringType, true),
            StructField("order_status",   StringType, true),
            StructField("order_category", StringType, true),
            StructField("order_date",     StringType, true),
            StructField("amount",         StringType, true)
          )
        )
      )
      .load(
        "dbfs:/Prophecy/109f794f881c8194b6a2b869509675e9/OrdersDatasetInput.csv"
      )

}
