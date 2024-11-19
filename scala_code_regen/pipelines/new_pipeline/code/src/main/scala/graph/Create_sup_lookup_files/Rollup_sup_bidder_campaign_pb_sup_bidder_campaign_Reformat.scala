package graph.Create_sup_lookup_files

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_sup_bidder_campaign_pb_sup_bidder_campaign_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      col("campaign_type_id").cast(IntegerType).as("campaign_type_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("campaign_group_type_id")
        .cast(IntegerType)
        .as("campaign_group_type_id")
    )

}
