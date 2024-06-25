package io.prophecy.pipelines.first.graph.Subgraph_1

import io.prophecy.libs._
import io.prophecy.pipelines.first.functions.PipelineInitCode._
import io.prophecy.pipelines.first.functions.UDFs._
import io.prophecy.pipelines.first.graph.Subgraph_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Limit_1 {
  def apply(context: Context, in: DataFrame): DataFrame = in.limit(10)
}
