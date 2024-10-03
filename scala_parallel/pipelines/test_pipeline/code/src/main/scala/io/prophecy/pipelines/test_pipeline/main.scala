package io.prophecy.pipelines.test_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.config._
import io.prophecy.pipelines.test_pipeline.config.ConfigStore.interimOutput
import io.prophecy.pipelines.test_pipeline.functions.UDFs._
import io.prophecy.pipelines.test_pipeline.functions.PipelineInitCode._
import io.prophecy.pipelines.test_pipeline.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_customers = customers(context).interim(
      "graph",
      "M32Ve7q6ehU7aAlXsxTOH$$Ajj-sWrJ9OQ3xeBp1NFwP",
      "GAl7MOAUD_0uhNYApSZ3d$$GYAZFmUfEksO4jNq_7sHs"
    )
    val df_orders = orders(context).interim(
      "graph",
      "rt5zMC8Vykn8_Uj2mGjyM$$Y1FHVht1uSadRzA-PQImr",
      "7koi9rnviaxZms9t6FzsX$$nPzQ4aQS3biPDK1RqUB5Y"
    )
    val df_join_orders_with_customers =
      join_orders_with_customers(context, df_orders, df_customers).interim(
        "graph",
        "TM-pOuMVIWG1ClUgwy0tP$$tz4298P9irpCDchGNpUmz",
        "gJ5SFYXuhHyh3eSDw52ac$$fIhky90WlVJxKM7fXrpfx"
      )
    val df_reformatted_data =
      reformatted_data(context, df_join_orders_with_customers).interim(
        "graph",
        "tF0E9Yr1aBgkya3_uOJNF$$732SwBeK45JHxHcQ22uo6",
        "3OCEelRZXsVXE9ignz8YG$$3wPiNpfSVZQ8F5Nx2BPWt"
      )
    val df_reformatted_orders =
      reformatted_orders(context, df_join_orders_with_customers).interim(
        "graph",
        "eszfzlPtbw_EGDWDRvwZ-$$bIhTJG6-_FG_XB2t8gaXx",
        "EzevCNMuIb2nPRY4SLaX6$$EekVVIMHaIO26VbZYAciA"
      )
    concurrent_execution_manager(context,
                                 df_reformatted_data,
                                 df_reformatted_orders
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test_pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/test_pipeline")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/test_pipeline") {
      spark.withSparkOptimisationsDisabled(graph(context))
    }
  }

}
