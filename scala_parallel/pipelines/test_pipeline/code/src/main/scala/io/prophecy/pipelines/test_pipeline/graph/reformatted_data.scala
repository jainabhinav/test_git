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

object reformatted_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(ceil(col("amount")).as("amount"),
              upper(col("first_name")).as("first_name")
    )

}
