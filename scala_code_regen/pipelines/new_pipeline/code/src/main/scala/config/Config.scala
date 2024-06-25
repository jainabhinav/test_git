package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config
}
import graph.Create_sup_lookup_files.config.{
  Config => Create_sup_lookup_files_Config
}
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config.{
  Config => xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config
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
import graph.Router_Reformatter.config.{Config => Router_Reformatter_Config}
import graph.LookupToJoin30.config.{Config => LookupToJoin30_Config}
import graph.LookupToJoin50.config.{Config => LookupToJoin50_Config}
import graph.LookupToJoin60.config.{Config => LookupToJoin60_Config}

case class Config(
  CHECKPOINT_STRATEGY: String = "checkpoint",
  XR_BUSINESS_DATE:    String = "20190101",
  XR_ADI_MEMBER_INFO:  String = "",
  XR_LOOKUP_DATA:      String = "hdfs:/app_abinitio/dev",
  XR_BUSINESS_HOUR:    String = "10",
  xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time_Config(),
  LookupToJoin30: LookupToJoin30_Config = LookupToJoin30_Config(),
  LookupToJoin60: LookupToJoin60_Config = LookupToJoin60_Config(),
  xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time_Config(),
  xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time_Config(),
  Router_Reformatter: Router_Reformatter_Config = Router_Reformatter_Config(),
  xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time_Config(),
  Create_sup_lookup_files: Create_sup_lookup_files_Config =
    Create_sup_lookup_files_Config(),
  xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time_Config(),
  LookupToJoin50: LookupToJoin50_Config = LookupToJoin50_Config(),
  xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time: xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time_Config =
    xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time_Config()
) extends ConfigBase