package io.prophecy.pipelines.first_agg_platform_video_analytics_hourly

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.config._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val (df_Create_sup_lookup_files_out3,
         df_Create_sup_lookup_files_out2,
         df_Create_sup_lookup_files_out1
    ) = Create_sup_lookup_files.apply(
      Create_sup_lookup_files.config
        .Context(context.spark, context.config.Create_sup_lookup_files)
    )
    val df_repartition_dataframe =
      repartition_dataframe(context, df_Create_sup_lookup_files_out1)
    SetCheckpointDir(context)
    val df_repartition_to_200 =
      repartition_to_200(context, df_Create_sup_lookup_files_out3)
    val df_repartition_by_inventory_url_id =
      repartition_by_inventory_url_id(context, df_Create_sup_lookup_files_out2)
    val df_Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics =
      Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics(
        context
      )
    val df_repartition_by_auction_id = repartition_by_auction_id(
      context,
      df_Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics
    )
    val df_Reformat_agg_platform_video_analytics_PrevExpressionj0 =
      Reformat_agg_platform_video_analytics_PrevExpressionj0(
        context,
        df_repartition_by_auction_id,
        df_repartition_dataframe
      )
    val df_Reformat_agg_platform_video_analytics_PrevExpression =
      Reformat_agg_platform_video_analytics_PrevExpression(
        context,
        df_Reformat_agg_platform_video_analytics_PrevExpressionj0
      )
    val df_Reformat_agg_platform_video_analyticsj0 =
      Reformat_agg_platform_video_analyticsj0(
        context,
        df_Reformat_agg_platform_video_analytics_PrevExpression,
        df_repartition_by_inventory_url_id,
        df_repartition_to_200
      )
    val df_Reformat_agg_platform_video_analytics =
      Reformat_agg_platform_video_analytics(
        context,
        df_Reformat_agg_platform_video_analyticsj0
      )
    val df_Pre_Rollup_agg_platform_video_analytics_hourly_pb =
      Pre_Rollup_agg_platform_video_analytics_hourly_pb(
        context,
        df_Reformat_agg_platform_video_analytics
      )
    val df_Reformat_agg_platform_video_analytics_hourly_pb =
      Reformat_agg_platform_video_analytics_hourly_pb(
        context,
        df_Pre_Rollup_agg_platform_video_analytics_hourly_pb
      )
    Write_Proto_HDFS_agg_platform_video_analytics_hourly_pb_agg_platform_video_analytics_hourly(
      context,
      df_Reformat_agg_platform_video_analytics_hourly_pb
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/first_agg_platform_video_analytics_hourly"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(
      spark,
      "pipelines/first_agg_platform_video_analytics_hourly"
    ) {
      apply(context)
    }
  }

}
