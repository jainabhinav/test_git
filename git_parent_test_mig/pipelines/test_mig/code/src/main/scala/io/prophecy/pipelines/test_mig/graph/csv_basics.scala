package io.prophecy.pipelines.test_mig.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_mig.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object csv_basics {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(StructField("a", StringType, true),
                StructField("b", StringType, true),
                StructField("c", StringType, true)
          )
        )
      )
      .load("asdad")

}
