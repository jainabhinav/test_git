package io.prophecy.pipelines.first.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first.functions.PipelineInitCode._
import io.prophecy.pipelines.first.graph.Subgraph_1.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Subgraph_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    val df_Filter_1 = Filter_1(context, in0)
    val df_Limit_1  = Limit_1(context,  df_Filter_1)
    df_Limit_1
  }

}
