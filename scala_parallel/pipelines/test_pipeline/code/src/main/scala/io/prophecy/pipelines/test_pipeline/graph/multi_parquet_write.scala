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

object multi_parquet_write {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): Unit =
    context.instrument {
      val spark = context.spark
      import scala.concurrent.{Await, ExecutionContext, Future}
      import scala.collection.mutable.ListBuffer
      import org.apache.hadoop.conf.Configuration
      import org.apache.hadoop.fs.Path
      import org.apache.spark.sql.functions._
      import org.joda.time.format._
      import spark.sqlContext.implicits._
      import scala.collection.JavaConverters.mapAsJavaMapConverter
      import scala.util.Try
      def CONCURRENT_TASKS   = 2
      def concurrentExecutor = ConcurrentExecutor(CONCURRENT_TASKS)
      def targetConfigs_temp =
        List(
          TargetConfig(id = "in0",
                       alias = "in0",
                       path = "asdaf",
                       isPathAbsolute = "false",
                       partitionType = "hour",
                       partitionDateOrHour = "sgsd",
                       allowTargetOverwrite = "true"
          ),
          TargetConfig(
            id = "P69dK6oaw5Kv-9uvv4wHx$$2NOjcsli9H4_IQEILnzfp",
            alias = "in1",
            path = "sdgsdg",
            isPathAbsolute = "false",
            partitionType = "hour",
            partitionDateOrHour = "sdgsdg",
            allowTargetOverwrite = "true"
          )
        )
      def tasks =
        targetConfigs_temp
          .zip(List(in0, in1))
          .map({
            case (x, y) =>
              def HOUR:                   String  = "hour".toLowerCase
              def pathArg:                String  = x.path.trim
              def isPathAbsoluteArg:      Boolean = x.isPathAbsolute == "true"
              def partitionDateOrHourArg: String  = x.partitionDateOrHour.trim
              def isHourlyPartition:      Boolean = x.partitionType == "hour"
              def allowTargetOverwriteArg: Boolean =
                x.allowTargetOverwrite == "true"
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
                              else
                                DateTimeFormat.forPattern(
                                  "yyyy-MM-dd 00:00:00"
                                )).parseDateTime(partitionDateOrHour)
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
              Future(y.write.format("parquet").save(outputPath))
          })
      concurrentExecutor.waitForResult(tasks)
      concurrentExecutor.shutdown()
      Console.out.println("Multi Parquet Write successful")
      Console.err.println("Multi Parquet Write successful")
    }

}
