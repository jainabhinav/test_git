package io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_Proto_Range_sup_api_member_pb {

  def apply(context: Context): DataFrame = {
    val spark  = context.spark
    val Config = context.config
    import org.apache.hadoop.conf._
    import org.apache.hadoop.fs._
    import org.apache.hadoop.io._
    import org.apache.spark.sql.protobuf.functions._
    import org.joda.time.format._
    import spark.sqlContext.implicits._
    import scala.collection.JavaConverters.mapAsJavaMapConverter
    import scala.util.{Failure, Success, Try}
    def pathArg:               String  = Config.datasets.inputs.sup_api_member_pb.trim
    def isPathAbsoluteArg:     Boolean = false
    def protobufSchemaNameArg: String  = "sup_api_member_dde".trim
    def protobufDescriptorFileNameArg: String =
      Config.datasets.hdfsProtoDescriptor.trim
    def partitionDateOrHourArg: String = Config.system.startDate.trim
    def isHourlyPartition: Boolean =
      "Hour".trim.toLowerCase == "hour".toLowerCase
    def fromArg:            Int     = if ("".trim.isEmpty) 0 else "".trim.toInt
    def toArg:              Int     = if ("".trim.isEmpty) 0 else "".trim.toInt
    def allowEmptyInputArg: Boolean = false
    def getPartitionedInputPaths(
      inputPath:             String,
      partitionDateOrHour:   String,
      from:                  Int,
      to:                    Int,
      isHourlyPartitionType: Boolean
    ): List[String] =
      Try {
        val dateTime = (if (isHourlyPartitionType)
                          DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
                        else DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00"))
          .parseDateTime(partitionDateOrHour)
        (for (index <- from to to)
          yield
            if (isHourlyPartitionType) dateTime.minusHours(index.abs)
            else dateTime.minusDays(index.abs))
          .map(suffix =>
            new Path(inputPath,
                     (if (isHourlyPartitionType)
                        DateTimeFormat.forPattern("yyyy/MM/dd/HH")
                      else
                        DateTimeFormat.forPattern("yyyy/MM/dd")).print(suffix)
            ).toString()
          )
          .toList
      }.get
    def verifyDirExistsAndHasData(
      pathString:    String,
      configuration: Configuration
    ): Try[String] = {
      val path = new Path(pathString)
      val fs   = path.getFileSystem(configuration)
      if (!fs.exists(path))
        Failure(new RuntimeException(s"Non-existent path: $path"))
      else if (!fs.getFileStatus(path).isDirectory)
        Failure(new RuntimeException(s"Non-existent directory: $path"))
      else {
        def filter(): PathFilter =
          p =>
            !p.getName.startsWith("_") && !p.getName
              .startsWith(".") && fs.getFileStatus(p).isFile
        if (fs.listStatus(path, filter()).nonEmpty) Success(pathString)
        else
          Failure(
            new RuntimeException(
              s"Directory does not have any data files in it: $path"
            )
          )
      }
    }
    val msg = s"""MicrosoftProtobufReader:
           |Path:$pathArg,
           |IsPathAbsolute:$isPathAbsoluteArg,
           |ProtobufSchema:$protobufSchemaNameArg,
           |ProtobufDescriptor:$protobufDescriptorFileNameArg,
           |PartitionDateOrHour:$partitionDateOrHourArg,
           |IsHour:$isHourlyPartition,
           |OffsetFromHour:$fromArg,
           |OffsetToHour:$toArg,
           |""".stripMargin.replaceAllLiterally("""
""", " ")
    Console.out.println(msg)
    Console.err.println(msg)
    val inputPaths =
      if (isPathAbsoluteArg) List(pathArg)
      else
        getPartitionedInputPaths(pathArg,
                                 partitionDateOrHourArg,
                                 fromArg,
                                 toArg,
                                 isHourlyPartition
        )
    inputPaths.foreach { p =>
      Console.out.println(s"MicrosoftProtobufReader: Input Paths:$p")
      Console.err.println(s"MicrosoftProtobufReader: Input Paths:$p")
    }
    val descriptorDF =
      spark.read.format("binaryFile").load(protobufDescriptorFileNameArg)
    val descriptorByteArray =
      descriptorDF.select("content").collect()(0)(0).asInstanceOf[Array[Byte]]
    if (!allowEmptyInputArg)
      inputPaths.foreach(path =>
        verifyDirExistsAndHasData(path,
                                  spark.sparkContext.hadoopConfiguration
        ).get
      )
    val binaryDF = inputPaths.map(path =>
      spark.sparkContext
        .sequenceFile(path, classOf[NullWritable], classOf[BytesWritable])
        .map(_._2.copyBytes())
        .toDF
    )
    val binaryUnionDF = binaryDF.reduce(_ union _)
    val options       = Map("enums.as.ints" -> "true").asJava
    binaryUnionDF
      .select(
        from_protobuf($"value",
                      protobufSchemaNameArg,
                      descriptorByteArray,
                      options
        ).as("protobuf_record")
      )
      .select("protobuf_record.*")
  }

}
