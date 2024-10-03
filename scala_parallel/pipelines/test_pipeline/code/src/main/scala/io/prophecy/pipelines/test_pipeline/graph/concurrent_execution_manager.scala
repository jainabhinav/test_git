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
    val t1 = Future(in0.write
            .format("delta")
            .option("overwriteSchema", true)
            .mode("overwrite")
            .saveAsTable("`abhinav_demo`.`test_scala_parallel1`"))
    
    
    val t2 = Future(in1.write
            .format("delta")
            .option("overwriteSchema", true)
            .mode("overwrite")
            .saveAsTable("`abhinav_demo`.`test_scala_parallel2`"))
    
    in0.write
            .format("delta")
            .option("overwriteSchema", true)
            .mode("overwrite")
            .saveAsTable("`abhinav_demo`.`test_scala_parallel3`")
    
    in1.write
            .format("delta")
            .option("overwriteSchema", true)
            .mode("overwrite")
            .saveAsTable("`abhinav_demo`.`test_scala_parallel4`")
    
    val tasks = List(t1, t2) // change
    concurrentExecutor.waitForResult(tasks) // change
    concurrentExecutor.shutdown() // change
  }

}
