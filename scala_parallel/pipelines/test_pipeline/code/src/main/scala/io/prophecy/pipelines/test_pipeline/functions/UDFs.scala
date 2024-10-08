package io.prophecy.pipelines.test_pipeline.functions

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    try registerAllUDFs(spark)
    catch {
      case _ => ()
    }
  }

}

object PipelineInitCode extends Serializable {
  import java.util.concurrent.atomic.AtomicInteger
  import java.util.concurrent.{Executors, ThreadFactory}
  import scala.concurrent.duration.Duration
  import scala.concurrent.{Await, ExecutionContext, Future}

  case class TargetConfig(
    id:                   String,
    alias:                String,
    path:                 String = "",
    isPathAbsolute:       String = "false",
    partitionType:        String = "hour",
    partitionDateOrHour:  String = "",
    allowTargetOverwrite: String = "true"
  )

  case class ConcurrentExecutor(concurrentTasks: Int) {

    private val pool = Executors.newFixedThreadPool(
      concurrentTasks,
      new ThreadFactory {
        private val threadNumber   = new AtomicInteger(1)
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

    implicit private val ec: ExecutionContext =
      ExecutionContext.fromExecutor(pool)

    def executionContext(): ExecutionContext = ec
    def shutdown():         Unit             = pool.shutdownNow()

    def waitForResult[T](list: List[Future[T]]): List[T] =
      Await.result(flattenListWithFailFastFuture(list), Duration.Inf)

    private def flattenListWithFailFastFuture[T](
      list: List[Future[T]]
    ): Future[List[T]] = {
      list.foldLeft(Future.successful(List.empty[T])) { (acc, future) =>
        acc.flatMap(results => future.map(result => results :+ result))
      }
    }

  }

  val CONCURRENT_TASKS   = 3
  val concurrentExecutor = ConcurrentExecutor(CONCURRENT_TASKS)
  implicit val ec: ExecutionContext = concurrentExecutor.executionContext()
}
