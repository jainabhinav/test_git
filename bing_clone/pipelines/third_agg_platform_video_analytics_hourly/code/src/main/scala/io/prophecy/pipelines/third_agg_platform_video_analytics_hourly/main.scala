package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.config._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val (df_Create_sup_lookup_files_out2, df_Create_sup_lookup_files_out1) =
      Create_sup_lookup_files.apply(
        Create_sup_lookup_files.config
          .Context(context.spark, context.config.Create_sup_lookup_files)
      )
    val df_repartition_dataframe =
      repartition_dataframe(context, df_Create_sup_lookup_files_out1)
    val df_Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics =
      Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics(
        context
      )
    val df_Filter_valid_pods = Filter_valid_pods(
      context,
      df_Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics
    )
    val df_repartition_by_auction_id_1 =
      repartition_by_auction_id_1(context, df_Filter_valid_pods)
    SetCheckpointDir(context)
    val df_repartition_by_inventory_url_id =
      repartition_by_inventory_url_id(context, df_Create_sup_lookup_files_out2)
    val df_Reformat_agg_platform_video_analytics_1_PrevExpressionj0 =
      Reformat_agg_platform_video_analytics_1_PrevExpressionj0(
        context,
        df_repartition_by_auction_id_1,
        df_repartition_dataframe
      )
    val df_Reformat_agg_platform_video_analytics_1_PrevExpression =
      Reformat_agg_platform_video_analytics_1_PrevExpression(
        context,
        df_Reformat_agg_platform_video_analytics_1_PrevExpressionj0
      )
    val df_Reformat_agg_platform_video_analytics_1j0 =
      Reformat_agg_platform_video_analytics_1j0(
        context,
        df_Reformat_agg_platform_video_analytics_1_PrevExpression,
        df_repartition_by_inventory_url_id
      )
    val df_Reformat_agg_platform_video_analytics_1 =
      Reformat_agg_platform_video_analytics_1(
        context,
        df_Reformat_agg_platform_video_analytics_1j0
      )
    val df_Pre_Rollup_agg_platform_video_slot_analytics_pb =
      Pre_Rollup_agg_platform_video_slot_analytics_pb(
        context,
        df_Reformat_agg_platform_video_analytics_1
      )
    val df_Reformat_agg_platform_video_slot_analytics_pb =
      Reformat_agg_platform_video_slot_analytics_pb(
        context,
        df_Pre_Rollup_agg_platform_video_slot_analytics_pb
      )
    Write_Proto_HDFS_agg_platform_video_slot_analytics_pb(
      context,
      df_Reformat_agg_platform_video_slot_analytics_pb
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
                   "pipelines/third_agg_platform_video_analytics_hourly"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(
      spark,
      "pipelines/third_agg_platform_video_analytics_hourly"
    ) {
      apply(context)
    }
  }

}
