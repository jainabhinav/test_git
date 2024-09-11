package io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_agg_platform_video_analytics_hourly_pb_agg_platform_video_analytics_hourly {

  def apply(context: Context, in: DataFrame): Unit = {
    val spark  = context.spark
    val Config = context.config
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path
    import org.apache.spark.sql.functions._
    import org.joda.time.format._
    import spark.sqlContext.implicits._
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    import scala.util.Try
    def HOUR: String = "hour".toLowerCase
    def pathArg: String =
      Config.datasets.outputs.agg_platform_video_analytics_hourly_pq.trim
    def isPathAbsoluteArg:      Boolean = false
    def partitionDateOrHourArg: String  = Config.system.startDate.trim
    def isHourlyPartition: Boolean =
      "hour".trim.toLowerCase == "hour".toLowerCase
    def allowTargetOverwriteArg: Boolean = true
    def getPartitionedOutputPaths(
      inputPath:             String,
      partitionDateOrHour:   String,
      isHourlyPartitionType: Boolean
    ): String =
      Try(
        new Path(inputPath,
                 (if (isHourlyPartitionType)
                    DateTimeFormat.forPattern("yyyy/MM/dd/HH")
                  else DateTimeFormat.forPattern("yyyy/MM/dd")).print(
                   (if (isHourlyPartitionType)
                      DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
                    else DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00"))
                     .parseDateTime(partitionDateOrHour)
                 )
        ).toString()
      ).get
    def prepareOutputDir(
      pathString:           String,
      allowTargetOverwrite: Boolean,
      configuration:        Configuration
    ): Unit = {
      val path = new Path(pathString)
      val fs   = path.getFileSystem(configuration)
      if (fs.exists(path) && fs.getFileStatus(path).isDirectory)
        if (allowTargetOverwrite) fs.delete(path, true)
      fs.mkdirs(path.getParent)
    }
    val msg = s"""MicrosoftMultiParquetWriter:
           |Path:$pathArg,
           |IsPathAbsolute:$isPathAbsoluteArg,
           |PartitionDateOrHour:$partitionDateOrHourArg,
           |IsHour:$isHourlyPartition,
           |""".stripMargin.replaceAllLiterally("""
""", " ")
    Console.out.println(msg)
    Console.err.println(msg)
    val outputPath =
      if (isPathAbsoluteArg) pathArg
      else
        getPartitionedOutputPaths(pathArg,
                                  partitionDateOrHourArg,
                                  isHourlyPartition
        )
    Console.out.println(
      s"MicrosoftMultiParquetReader: Output Paths:$outputPath"
    )
    Console.err.println(
      s"MicrosoftMultiParquetReader: Output Paths:$outputPath"
    )
    prepareOutputDir(outputPath,
                     allowTargetOverwriteArg,
                     spark.sparkContext.hadoopConfiguration
    )
    in.write.format("parquet").save(outputPath)
  }

}
