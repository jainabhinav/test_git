package io.prophecy.pipelines.test_pipeline

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.config._
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

  def apply(context: Context): Unit = {
    val df_customers = customers(context)
    val df_orders    = orders(context)
    val df_join_orders_with_customers =
      join_orders_with_customers(context, df_orders, df_customers)
    val df_reformatted_data =
      reformatted_data(context, df_join_orders_with_customers)
    val df_reformatted_orders =
      reformatted_orders(context, df_join_orders_with_customers)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/test_pipeline")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/test_pipeline") {
      apply(context)
    }
  }

}
