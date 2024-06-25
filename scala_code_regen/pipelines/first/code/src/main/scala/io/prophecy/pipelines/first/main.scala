package io.prophecy.pipelines.first

import io.prophecy.libs._
import io.prophecy.pipelines.first.config._
import io.prophecy.pipelines.first.functions.UDFs._
import io.prophecy.pipelines.first.functions.PipelineInitCode._
import io.prophecy.pipelines.first.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dummy            = dummy(context)
    val df_reformat_columns = reformat_columns(context, df_dummy)
    val df_Subgraph_1 = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_reformat_columns
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("first")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/first")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/first") {
      apply(context)
    }
  }

}
