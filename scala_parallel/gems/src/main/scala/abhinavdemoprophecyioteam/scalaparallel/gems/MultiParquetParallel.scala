package prophecydemos.paralleljobstestscala.gems

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.componentSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}

class MultiParquetParallel extends ComponentSpec {

  val name: String = "MultiParquetParallel"
  val category: String = "Custom"
  type PropertiesType = MultiParquetParallelProperties
  override def optimizeCode: Boolean = true

  case class MultiParquetParallelProperties(
      @Property("targetConfigs", "List of target configs")
      targetConfigs: List[TargetConfig] = List(
        TargetConfig(
          "in0",
          "in0",
          SString(""),
          "false",
          "hour",
          SString(""),
          "true"
        )
      ),
      @Property("concurrentTasks")
      concurrentTasks: String = "2"
  ) extends ComponentProperties

  @Property("TargetConfig")
  case class TargetConfig(
      @Property("Id")
      id: String,
      @Property("Input port")
      alias: String,
      @Property("path")
      path: SString = SString(""),
      @Property("isPathAbsolute")
      isPathAbsolute: String = "false",
      @Property("partitionType")
      partitionType: String = "hour",
      @Property("partitionDateOrHour")
      partitionDateOrHour: SString = SString(""),
      @Property("allowTargetOverwrite")
      allowTargetOverwrite: String = "true"
  )

  implicit val targetConfigFormat: Format[TargetConfig] = Json.format

  implicit val MultiParquetParallelPropertiesFormat
      : Format[MultiParquetParallelProperties] = Json.format

  def dialog: Dialog = {

    val selectBoxTF = SelectBox("")
      .addOption("true", "true")
      .addOption("false", "false")

    val selectBoxDH = SelectBox("")
      .addOption("day", "day")
      .addOption("hour", "hour")

    val targetConfigsTable = BasicTable(
      "Test",
      columns = List(
        Column("Input Alias", "alias", width = "10%"),
        Column(
          "Path prefix",
          "path",
          Some(TextBox("").bindPlaceholder("hdfs:/...")),
          width = "35%"
        ),
        Column(
          "Absolute path",
          "isPathAbsolute",
          Some(selectBoxTF),
          width = "13%"
        ),
        Column(
          "Partition type",
          "partitionType",
          Some(selectBoxDH),
          width = "13%"
        ),
        Column(
          "Data partition",
          "partitionDateOrHour",
          Some(TextBox("").bindPlaceholder("2024-04-01 02:00:00")),
          width = "15%"
        ),
        Column(
          "Target Overwrite",
          "allowTargetOverwrite",
          Some(selectBoxTF),
          width = "13%"
        )
      ),
      targetColumnKey = Some("alias"),
      delete = false,
      appendNewRow = false
    ).bindProperty("targetConfigs")

    Dialog("MultiParquetParallel").addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          Ports(minInputPorts = 1)
            .editableInput(true),
          "content"
        )
        .addColumn(
          StackLayout(height = Some("100%"))
            .addElement(TextBox("Concurrent tasks").bindPlaceholder("2").bindProperty("concurrentTasks"))
            .addElement(targetConfigsTable),
          "2fr"
        )
    )
  }

  def validate(component: Component)(implicit
      context: WorkflowContext
  ): List[Diagnostic] = {

    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    component.properties.targetConfigs.zipWithIndex.foreach {
      case (targetConfig, idx) ⇒
        if (targetConfig.path.isEmpty)
          diagnostics += Diagnostic(
            s"properties.targetConfigs[$idx].path",
            "Path prefix cannot be empty.",
            SeverityLevel.Error
          )
        if (targetConfig.partitionDateOrHour.isEmpty)
          diagnostics += Diagnostic(
            s"properties.targetConfigs[$idx].partitionDateOrHour",
            "Partition date or hour cannot be empty.",
            SeverityLevel.Error
          )
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit
      context: WorkflowContext
  ): Component = {
    val newProps = newState.properties
    var targetConfigsAfterChangeApplication = List[TargetConfig]()

    for (newport ← newState.ports.inputs) {
      var flag = true
      for (targetConfig ← newState.properties.targetConfigs) {
        if (newport.id == targetConfig.id || newport.slug == targetConfig.alias) {
          targetConfigsAfterChangeApplication =
            targetConfigsAfterChangeApplication :+ TargetConfig(
              targetConfig.id,
              targetConfig.alias,
              targetConfig.path,
              targetConfig.isPathAbsolute,
              targetConfig.partitionType,
              targetConfig.partitionDateOrHour,
              targetConfig.allowTargetOverwrite
            )
          flag = false
        }
      }
      if (flag) {
        targetConfigsAfterChangeApplication =
          targetConfigsAfterChangeApplication :+ TargetConfig(
            newport.id,
            newport.slug,
            SString(""),
            "false",
            "hour",
            SString(""),
            "true"
          )
      }
    }

    newState.copy(
      properties = newProps.copy(
        targetConfigs = targetConfigsAfterChangeApplication
      ),
      ports = newState.ports.copy(isCustomOutputSchema = true)
    )
  }

  def deserializeProperty(props: String): MultiParquetParallelProperties =
    Json.parse(props).as[MultiParquetParallelProperties]

  def serializeProperty(props: MultiParquetParallelProperties): String =
    Json.toJson(props).toString()

  class MultiParquetParallelCode(props: PropertiesType) extends ComponentCode {
    import java.util.concurrent.atomic.AtomicInteger
    import java.util.concurrent.{Executors, ThreadFactory}
    import scala.concurrent.duration.Duration
    import scala.concurrent.{Await, ExecutionContext, Future}

    /** Utility that allow concurrent execution of spark stages.
      */
    case class ConcurrentExecutor(concurrentTasks: Int) {
      private val pool = Executors.newFixedThreadPool(
        concurrentTasks,
        new ThreadFactory {
          private val threadNumber = new AtomicInteger(1)
          private val defaultFactory = Executors.defaultThreadFactory()

          override def newThread(r: Runnable): Thread = {
            val thread = defaultFactory.newThread(r)
            thread.setName(
              s"spark-driver-concurrent-execution-thread-${threadNumber.getAndIncrement()}"
            )
            thread.setDaemon(true)
            thread
          }
        }
      )

      private implicit val ec: ExecutionContext =
        ExecutionContext.fromExecutor(pool)

      def executionContext(): ExecutionContext = ec

      def shutdown(): Unit = pool.shutdownNow()

      def waitForResult[T](list: List[Future[T]]): List[T] =
        Await.result(flattenListWithFailFastFuture(list), Duration.Inf)

      private def flattenListWithFailFastFuture[T](
          list: List[Future[T]]
      ): Future[List[T]] = {
        // Create a single future that fails on first failure (fail fast)
        list.foldLeft(Future.successful(List.empty[T])) { (acc, future) =>
          acc.flatMap(results => future.map(result => results :+ result))
        }
      }
    }

    def apply(spark: SparkSession, in: DataFrame*): Unit = {
      import scala.concurrent.{Await, ExecutionContext, Future}
      import scala.collection.mutable.ListBuffer
      import org.apache.hadoop.conf.Configuration
      import org.apache.hadoop.fs.Path
      import org.apache.spark.sql.functions._
      import org.joda.time.format._
      import spark.sqlContext.implicits._

      import scala.collection.JavaConverters.mapAsJavaMapConverter
      import scala.util.Try

      def CONCURRENT_TASKS = props.concurrentTasks.toInt
      def concurrentExecutor = ConcurrentExecutor(CONCURRENT_TASKS)
      implicit val ec: ExecutionContext = concurrentExecutor.executionContext()

      def targetConfigs_temp = props.targetConfigs

      def tasks = targetConfigs_temp.zip(in).map {
        case (x, y) => {
          def HOUR: String = "hour".toLowerCase

          def pathArg: String = x.path.trim

          def isPathAbsoluteArg: Boolean = x.isPathAbsolute == "true"

          def partitionDateOrHourArg: String = x.partitionDateOrHour.trim

          def isHourlyPartition: Boolean = x.partitionType == "hour"

          def allowTargetOverwriteArg: Boolean =
            x.allowTargetOverwrite == "true"

          def getPartitionedOutputPaths(
              inputPath: String,
              partitionDateOrHour: String,
              isHourlyPartitionType: Boolean
          ): String = {
            Try {
              val parser =
                if (isHourlyPartitionType)
                  DateTimeFormat.forPattern("yyyy-MM-dd HH:00:00")
                else DateTimeFormat.forPattern("yyyy-MM-dd 00:00:00")
              val printer =
                if (isHourlyPartitionType)
                  DateTimeFormat.forPattern("yyyy/MM/dd/HH")
                else DateTimeFormat.forPattern("yyyy/MM/dd")
              val dateTime = parser.parseDateTime(partitionDateOrHour)
              new Path(inputPath, printer.print(dateTime)).toString
            }.get
          }

          def prepareOutputDir(
              pathString: String,
              allowTargetOverwrite: Boolean,
              configuration: Configuration
          ): Unit = {
            val path = new Path(pathString)
            val fs = path.getFileSystem(configuration)
            if (fs.exists(path) && fs.getFileStatus(path).isDirectory) {
              if (allowTargetOverwrite) {
                // Recursive delete
                fs.delete(path, true)
              }
            }
            // create parent folders
            fs.mkdirs(path.getParent)
          }

          val msg =
            s"""MicrosoftMultiParquetWriter:
               |Path:$pathArg,
               |IsPathAbsolute:$isPathAbsoluteArg,
               |PartitionDateOrHour:$partitionDateOrHourArg,
               |IsHour:$isHourlyPartition,
               |""".stripMargin.replaceAllLiterally("\n", " ")
          Console.out.println(msg)
          Console.err.println(msg)

          val outputPath =
            if (isPathAbsoluteArg) pathArg
            else
              getPartitionedOutputPaths(
                pathArg,
                partitionDateOrHourArg,
                isHourlyPartition
              )
          Console.out
            .println(s"MicrosoftMultiParquetReader: Output Paths:$outputPath")
          Console.err
            .println(s"MicrosoftMultiParquetReader: Output Paths:$outputPath")
          prepareOutputDir(
            outputPath,
            allowTargetOverwriteArg,
            spark.sparkContext.hadoopConfiguration
          )

          Future(y.write.format("parquet").save(outputPath))
        }
      }

      concurrentExecutor.waitForResult(tasks)
      concurrentExecutor.shutdown()

      Console.out
        .println(s"Multi Parquet Write successful")
      Console.err
        .println(s"Multi Parquet Write successful")

    }
  }
}
