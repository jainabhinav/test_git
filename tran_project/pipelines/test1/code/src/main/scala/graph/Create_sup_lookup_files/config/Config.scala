package graph.Create_sup_lookup_files.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  var datasets:         Datasets = Datasets(),
  var XR_BUSINESS_DATE: String = "20200331",
  var XR_ADI_MEMBER_INFO: String =
    "dbfs:/FileStore/data_engg/msbing/transaction_graph/Write_Multiple_Files_memberCounts",
  var XR_LOOKUP_DATA:   String = "hdfs:/data_team/team_user_space/blee/prophecy",
  var XR_BUSINESS_HOUR: String = "05",
  var system:           System = System()
) extends ConfigBase

object Datasets {

  implicit val confHint: ProductHint[Datasets] =
    ProductHint[Datasets](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Datasets(
  var hdfsLogPrefix: String = "hdfs:/data_team/logs",
  var hdfsSupPrefix: String = "hdfs:/data_team/sup",
  var hdfsAggPrefix: String = "hdfs:/data_team/aggs",
  var inputs:        Inputs = Inputs(),
  var outputs:       Outputs = Outputs(),
  var hdfsProtoDescriptor: String =
    "hdfs:/data_team/artifacts/schemas-descriptor/current/schemas-descriptor.protobin"
)

object Inputs {

  implicit val confHint: ProductHint[Inputs] =
    ProductHint[Inputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Inputs(
  var log_impbus_impressions_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_impressions_pb/v000",
  var log_impbus_impressions_deferred_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_impressions_deferred_pb/v000",
  var log_impbus_imptracker_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_imptracker_pb/v000",
  var log_impbus_impressions_pricing_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_impressions_pricing_pb/v000",
  var log_impbus_impressions_deferred_pricing_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_impressions_deferred_pricing_pb/v000",
  var log_impbus_preempt_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_preempt_pb/v000",
  var log_impbus_preempt_deferred_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_preempt_deferred_pb/v000",
  var log_impbus_view_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_view_pb/v000",
  var log_impbus_auction_event_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_impbus_auction_event_pb/v000",
  var log_dw_bid_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_dw_bid_pb/v000",
  var log_dw_imptracker_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_dw_imptracker_pb/v000",
  var log_dw_view_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/logs/log_dw_view_pb/v000",
  var video_slot_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/validated_logs/video_slot_pb",
  var sup_ip_range_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_ip_range_pb",
  var sup_common_deal_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_common_deal",
  var sup_bidder_member_sales_tax_rate_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_bidder_member_sales_tax_rate_pb",
  var sup_bidder_insertion_order_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_bidder_insertion_order_pb",
  var sup_sell_side_object_hierarchy_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_sell_side_object_hierarchy_pb",
  var sup_bidder_campaign_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_bidder_campaign",
  var sup_placement_video_attributes_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_placement_video_attributes_pb",
  var sup_buy_side_object_hierarchy_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_buy_side_object_hierarchy_pb",
  var sup_tl_api_inventory_url_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/sup/sup_tl_api_inventory_url_pb"
)

object Outputs {

  implicit val confHint: ProductHint[Outputs] =
    ProductHint[Outputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Outputs(
  var agg_dw_curator_impressions_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/agg_dw_curator_impressions_pb",
  var stage_seen_transacted_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/stage_seen_transacted_pb",
  var stage_tl_trx_trans_denormalized_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/stage_tl_trx_trans_denormalized_pb",
  var stage_impbus_impression_sample_trans_path_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/stage_impbus_impression_sample_trans_path_pb",
  var agg_dw_impressions_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/agg_dw_impressions_pb",
  var stage_invalid_impressions_quarantine_pb: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/aggs/stage_invalid_impressions_quarantine_pb",
  var memberCounts: String =
    "hdfs:/data_team/team_user_space/blee/prophecy/filters/memberCounts"
)

object System {

  implicit val confHint: ProductHint[System] =
    ProductHint[System](ConfigFieldMapping(CamelCase, CamelCase))

}

case class System(
  var startDate:      String = "2020-03-31 05:00:00",
  var sparkSubmit:    Option[String] = None,
  var appJar:         Option[String] = None,
  var runtimeConf:    Option[String] = None,
  var sparkArguments: Option[String] = None
)

case class Context(spark: SparkSession, config: Config)
