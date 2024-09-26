package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Write_Proto_HDFS_IIPQ_stage_invalid_impressions_quarantine_pb_stage_invalid_impressions_quarantine {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("parquet")
      .mode("overwrite")
      .save(
        "dbfs:/FileStore/data_engg/msbing/transaction_graph/prophecy_output/stage_invalid_impressions_quarantine"
      )

}
