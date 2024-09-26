package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_by_Expression_agg_dw_curator_impressions_DropExtraColumns {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.drop("_member_id_by_advertiser_id_LOOKUP")
      .drop("_advertiser_id_by_campaign_group_id_LOOKUP")
      .drop("_member_id_by_publisher_id_LOOKUP")
      .withColumn("crossdevice_graph_membership",
                  when(size(col("crossdevice_graph_membership")) === 0,
                       lit(null)
                  ).otherwise(col("crossdevice_graph_membership"))
      )
      .withColumn("data_costs",
                  when(size(col("data_costs")) === 0, lit(null))
                    .otherwise(col("data_costs"))
      )
      .withColumn("gdpr_consent_string",
                  when(length(col("gdpr_consent_string")) === 0, lit(null))
                    .otherwise(col("gdpr_consent_string"))
      )
      .withColumn(
        "excluded_targeted_segment_details",
        when(size(col("excluded_targeted_segment_details")) === 0, lit(null))
          .otherwise(col("excluded_targeted_segment_details"))
      )
      .withColumn("targeted_segment_details",
                  when(size(col("targeted_segment_details")) === 0, lit(null))
                    .otherwise(col("targeted_segment_details"))
      )

}
