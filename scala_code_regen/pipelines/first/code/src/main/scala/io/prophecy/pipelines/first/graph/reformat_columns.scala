package io.prophecy.pipelines.first.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first.functions.PipelineInitCode._
import io.prophecy.pipelines.first.functions.UDFs._
import io.prophecy.pipelines.first.functions.ColumnFunctions._
import io.prophecy.pipelines.first.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object reformat_columns {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("a"),
              col("b"),
              col("c"),
              concat(col("a"), col("b"), col("c")).as("d")
    )

}
