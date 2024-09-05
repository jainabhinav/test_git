package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_agg_dw_clicktracker_agg_dw_clicktracker_pb {

  def apply(context: Context, in: DataFrame): Unit = {
    val spark  = context.spark
    val Config = context.config
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.io.{BytesWritable, NullWritable}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.protobuf.functions._
    import org.joda.time.format._
    import spark.sqlContext.implicits._
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    import scala.util.Try
    def HOUR:                  String  = "hour".toLowerCase
    def pathArg:               String  = Config.datasets.outputs.agg_dw_clicktracker_pb.trim
    def isPathAbsoluteArg:     Boolean = false
    def protobufSchemaNameArg: String  = "agg_dw_clicktracker".trim
    def protobufDescriptorFileNameArg: String =
      Config.datasets.hdfsProtoDescriptor.trim
    def partitionDateOrHourArg: String = Config.system.startDate.trim
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
    val msg = s"""MicrosoftProtobufWriter:
           |Path:$pathArg,
           |IsPathAbsolute:$isPathAbsoluteArg,
           |ProtobufSchema:$protobufSchemaNameArg,
           |ProtobufDescriptor:$protobufDescriptorFileNameArg,
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
    Console.out.println(s"MicrosoftProtobufReader: Output Paths:$outputPath")
    Console.err.println(s"MicrosoftProtobufReader: Output Paths:$outputPath")
    prepareOutputDir(outputPath,
                     allowTargetOverwriteArg,
                     spark.sparkContext.hadoopConfiguration
    )
    in.select(expr("struct(*) as msg"))
      .select(
        to_protobuf(
          $"msg",
          protobufSchemaNameArg,
          spark.read
            .format("binaryFile")
            .load(protobufDescriptorFileNameArg)
            .select("content")
            .collect()(0)(0)
            .asInstanceOf[Array[Byte]],
          Map("enums.as.ints" -> "true").asJava
        ).as("protobuf_record")
      )
      .toDF
      .rdd
      .map(row =>
        (NullWritable.get(),
         new BytesWritable(row.getAs[Array[Byte]]("protobuf_record"))
        )
      )
      .saveAsSequenceFile(
        outputPath,
        Some(classOf[org.apache.hadoop.io.compress.SnappyCodec])
      )
  }

}
