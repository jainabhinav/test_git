package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.{
  Config => Create_sup_lookup_files_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config
}
import graph.Router_Reformatter.config.{Config => Router_Reformatter_Config}
import graph.LookupToJoin50.config.{Config => LookupToJoin50_Config}

case class Config(
  var XR_BUSINESS_DATE: String = "20200331",
  var XR_ADI_MEMBER_INFO: String =
    "dbfs:/FileStore/data_engg/msbing/transaction_graph/Write_Multiple_Files_memberCounts",
  var XR_LOOKUP_DATA:   String = "hdfs:/data_team/team_user_space/blee/prophecy",
  var XR_BUSINESS_HOUR: String = "05",
  var xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config(),
  var xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time_Config(),
  var xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time_Config(),
  var Router_Reformatter: Router_Reformatter_Config =
    Router_Reformatter_Config(),
  var xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time_Config(),
  var Create_sup_lookup_files: Create_sup_lookup_files_Config =
    Create_sup_lookup_files_Config(),
  var xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config(),
  var LookupToJoin50: LookupToJoin50_Config = LookupToJoin50_Config(),
  var xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time_Config(),
  var datasets: Datasets = Datasets(),
  var jobs:     Option[Jobs] = None,
  var system:   System = System()
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

object Jobs {

  implicit val confHint: ProductHint[Jobs] =
    ProductHint[Jobs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Jobs(
  var trx_tl_ad_request_trans_path_core_processing_pb: Option[
    Trx_tl_ad_request_trans_path_core_processing_pb
  ] = None
)

object Trx_tl_ad_request_trans_path_core_processing_pb {

  implicit val confHint
    : ProductHint[Trx_tl_ad_request_trans_path_core_processing_pb] =
    ProductHint[Trx_tl_ad_request_trans_path_core_processing_pb](
      ConfigFieldMapping(CamelCase, CamelCase)
    )

}

case class Trx_tl_ad_request_trans_path_core_processing_pb(
  var sparkArguments: Option[String] = None,
  var sparkCommands:  Option[String] = None
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
