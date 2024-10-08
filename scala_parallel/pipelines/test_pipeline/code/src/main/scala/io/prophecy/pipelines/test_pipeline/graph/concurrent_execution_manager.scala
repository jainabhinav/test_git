package io.prophecy.pipelines.test_pipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.test_pipeline.config.Context
import io.prophecy.pipelines.test_pipeline.functions.UDFs._
import io.prophecy.pipelines.test_pipeline.functions.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object concurrent_execution_manager {
  def apply(context: Context, in0: DataFrame, in1: DataFrame): Unit = context.instrument {
    val spark = context.spark
    val Config = context.config
    import scala.concurrent.{Await, ExecutionContext, Future}
    import scala.collection.mutable.ListBuffer
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path
    import org.apache.spark.sql.functions._
    import org.joda.time.format._
    import spark.sqlContext.implicits._
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    import scala.util.Try
    def CONCURRENT_TASKS   = 4
    def concurrentExecutor = ConcurrentExecutor(CONCURRENT_TASKS)
    def targetConfigs_temp =
      List(
        TargetConfig(id = "in0",
                     alias = "in0",
                     path = "`abhinav_demo`.`test_scala_parallel1`",
                     isPathAbsolute = "false",
                     partitionType = "hour",
                     partitionDateOrHour = "sdg",
                     allowTargetOverwrite = "true"
        ),
        TargetConfig(
          id = "sWyrPmba0uj7Av6YL5QXl$$yBykcWsp6JMpuNJQkGje4",
          alias = "in1",
          path = "`abhinav_demo`.`test_scala_parallel2`",
          isPathAbsolute = "false",
          partitionType = "hour",
          partitionDateOrHour = "sdf",
          allowTargetOverwrite = "true"
        ),
        TargetConfig(
          id = "sWyrPmba0uj7Av6YL5QXl$$yBykcWsp6JMpuNJQkGje4",
          alias = "in1",
          path = "`abhinav_demo`.`test_scala_parallel3`",
          isPathAbsolute = "false",
          partitionType = "hour",
          partitionDateOrHour = "sdf",
          allowTargetOverwrite = "true"
        ),
        TargetConfig(
          id = "sWyrPmba0uj7Av6YL5QXl$$yBykcWsp6JMpuNJQkGje4",
          alias = "in1",
          path = "`abhinav_demo`.`test_scala_parallel4`",
          isPathAbsolute = "false",
          partitionType = "hour",
          partitionDateOrHour = "sdf",
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
    //         def partitionDateOrHourArg: String  = x.partitionDateOrHour.trim
    //         def isHourlyPartition:      Boolean = x.partitionType == "hour"
    //         def allowTargetOverwriteArg: Boolean =
    //           x.allowTargetOverwrite == "true"
    //         def getPartitionedOutputPaths(
    //           inputPath:             String,
    //           partitionDateOrHour:   String,
    //           isHourlyPartitionType: Boolean
    //         ): String =
    //           Try(
    //             new Path(inputPath,
    //                      (if (isHourlyPartitionType)
    //                         DateTimeFormat.forPattern("yyyy/MM/dd/HH")
    //                       else DateTimeFormat.forPattern("yyyy/MM/dd")).print(
    //                        (if (isHourlyPartitionType)
    //                           DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
    //                         else
    //                           DateTimeFormat.forPattern(
    //                             "yyyy-MM-dd 00:00:00"
    //                           )).parseDateTime(partitionDateOrHour)
    //                      )
    //             ).toString()
    //           ).get
    //         def prepareOutputDir(
    //           pathString:           String,
    //           allowTargetOverwrite: Boolean,
    //           configuration:        Configuration
    //         ): Unit = {
    //           val path = new Path(pathString)
    //           val fs   = path.getFileSystem(configuration)
    //           if (fs.exists(path) && fs.getFileStatus(path).isDirectory)
    //             if (allowTargetOverwrite) fs.delete(path, true)
    //           fs.mkdirs(path.getParent)
    //         }
    //         val msg = s"""MicrosoftMultiParquetWriter:
    //          |Path:$pathArg,
    //          |IsPathAbsolute:$isPathAbsoluteArg,
    //          |PartitionDateOrHour:$partitionDateOrHourArg,
    //          |IsHour:$isHourlyPartition,
    //          |""".stripMargin.replaceAllLiterally("""
    // """, " ")
    //         Console.out.println(msg)
    //         Console.err.println(msg)
    //         val outputPath =
    //           if (isPathAbsoluteArg) pathArg
    //           else
    //             getPartitionedOutputPaths(pathArg,
    //                                       partitionDateOrHourArg,
    //                                       isHourlyPartition
    //             )
    //         Console.out.println(
    //           s"MicrosoftMultiParquetReader: Output Paths:$outputPath"
    //         )
    //         Console.err.println(
    //           s"MicrosoftMultiParquetReader: Output Paths:$outputPath"
    //         )
    //         prepareOutputDir(outputPath,
    //                          allowTargetOverwriteArg,
    //                          spark.sparkContext.hadoopConfiguration
    //         )
            Future(y.write
            .format("delta")
            .option("overwriteSchema", true)
            .mode("overwrite")
            .saveAsTable(pathArg))
        })
    concurrentExecutor.waitForResult(tasks)
    concurrentExecutor.shutdown()
    Console.out.println("Multi Parquet Write successful")
    Console.err.println("Multi Parquet Write successful")
  }

}
