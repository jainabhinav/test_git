package udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("temp1151664_UDF", temp1151664_UDF)
    spark.udf.register("temp2545540_UDF", temp2545540_UDF)
    spark.udf.register("temp3256870_UDF", temp3256870_UDF)
    spark.udf.register("temp634656_UDF",  temp634656_UDF)
    spark.udf.register("temp1344271_UDF", temp1344271_UDF)
    spark.udf.register("temp2053714_UDF", temp2053714_UDF)
    spark.udf.register("temp2762257_UDF", temp2762257_UDF)
    spark.udf.register("temp3471027_UDF", temp3471027_UDF)
    spark.udf.register("temp4180587_UDF", temp4180587_UDF)
    spark.udf.register("temp6282792_UDF", temp6282792_UDF)
    spark.udf.register("temp6995147_UDF", temp6995147_UDF)
    spark.udf.register("rollup1615_UDF",  rollup1615_UDF)
    spark.udf.register("rollup1621_UDF",  rollup1621_UDF)
    spark.udf.register("rollup1627_UDF",  rollup1627_UDF)
    spark.udf.register(
      "f_create_agg_dw_impressions_virtual_log_dw_bid_10949800",
      f_create_agg_dw_impressions_virtual_log_dw_bid_10949800
    )
    registerAllUDFs(spark)
  }

  def temp1151664_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _term_id:         Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer
      ) => {
        var term_id         = _term_id
        var i               = _i
        var l_pricing_term  = Row(null, null, null, null, null, null)
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (l_term_id == term_id)
            l_pricing_term = l_pricing_terms(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(
          if (!_isnull(l_pricing_term))
            Row(
              convertToInt(l_pricing_term.get(0)),
              l_pricing_term.get(1),
              l_pricing_term.get(2),
              l_pricing_term.get(3),
              l_pricing_term.get(4),
              convertToInt(l_pricing_term.get(5))
            )
          else
            null,
          convertToInt(i),
          convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField("i",         IntegerType, false),
          StructField("l_term_id", IntegerType, false)
        )
      )
    )
  }

  def temp2545540_UDF = {
    udf(
      (
        _in_pricing_terms: Seq[Row],
        _l_pricing_terms:  Seq[Row],
        _i:                Integer,
        _magnitude:        Double
      ) => {
        var l_pricing_terms  = _l_pricing_terms.toArray
        var i                = _i
        var in_pricing_terms = _in_pricing_terms.toArray
        var magnitude        = _magnitude
        while (compareTo(i, l_pricing_terms.length) < 0) {
          if (
            !_isnull(in_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
          )
            l_pricing_terms(i) = Row(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
              l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount") / magnitude,
              l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
              l_pricing_terms(convertToInt(i))
                .getAs[Boolean]("is_media_cost_dependent"),
              l_pricing_terms(convertToInt(i)).getAs[Integer]("data_member_id")
            )
          i = i + convertToInt(1)
        }
        Row(
          l_pricing_terms.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.get(0)),
                  x.get(1),
                  x.get(2),
                  x.get(3),
                  x.get(4),
                  convertToInt(x.get(5))
              )
            else
              null
          }.toArray,
          convertToInt(i)
        )
      },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp3256870_UDF = {
    udf(
      (_l_filtered_terms: Seq[Row], _unfiltered_terms: Seq[Row], _i: Integer) =>
        {
          var l_current_term   = Row(null, null, null, null, null, null)
          var l_filtered_terms = _l_filtered_terms.toArray
          var i                = _i
          var unfiltered_terms = _unfiltered_terms.toArray
          while (compareTo(i, unfiltered_terms.length) < 0) {
            l_current_term = unfiltered_terms(convertToInt(i))
            if (
              unfiltered_terms(convertToInt(i))
                .getAs[Boolean]("is_deduction") == convertToBoolean(0)
            )
              l_filtered_terms =
                Array.concat(l_filtered_terms,
                             Array.fill(1)(unfiltered_terms(convertToInt(i)))
                )
            i = i + convertToInt(1)
          }
          Row(
            if (!_isnull(l_current_term))
              Row(
                convertToInt(l_current_term.get(0)),
                l_current_term.get(1),
                l_current_term.get(2),
                l_current_term.get(3),
                l_current_term.get(4),
                convertToInt(l_current_term.get(5))
              )
            else
              null,
            l_filtered_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.get(0)),
                    x.get(1),
                    x.get(2),
                    x.get(3),
                    x.get(4),
                    convertToInt(x.get(5))
                )
              else
                null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_current_term",
            StructType(
              List(
                StructField("term_id",                 IntegerType, true),
                StructField("amount",                  DoubleType,  true),
                StructField("rate",                    DoubleType,  true),
                StructField("is_deduction",            BooleanType, true),
                StructField("is_media_cost_dependent", BooleanType, true),
                StructField("data_member_id",          IntegerType, true)
              )
            ),
            false
          ),
          StructField(
            "l_filtered_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp634656_UDF = {
    udf(
      (
        _i:                Integer,
        _l_curator_margin: Double,
        _l_is_deduction:   Integer,
        _l_pricing_terms:  Seq[Row],
        _l_term_id:        Integer
      ) => {
        var i                = _i
        var l_curator_margin = _l_curator_margin
        var l_is_deduction   = _l_is_deduction
        var l_pricing_terms  = _l_pricing_terms.toArray
        var l_term_id        = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_curator_margin = l_curator_margin + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            l_curator_margin
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",   IntegerType, false),
          StructField("i",                IntegerType, false),
          StructField("l_term_id",        IntegerType, false),
          StructField("l_curator_margin", DoubleType,  false)
        )
      )
    )
  }

  def temp1344271_UDF = {
    udf(
      (
        _l_total_tech_fees: Double,
        _i:                 Integer,
        _l_is_deduction:    Integer,
        _l_pricing_terms:   Seq[Row],
        _l_term_id:         Integer
      ) => {
        var l_total_tech_fees = _l_total_tech_fees
        var i                 = _i
        var l_is_deduction    = _l_is_deduction
        var l_pricing_terms   = _l_pricing_terms.toArray
        var l_term_id         = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (
            Array(99, 89, 88, 85, 86).contains(l_term_id) && l_is_deduction == 1
          ) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_total_tech_fees = l_total_tech_fees + l_pricing_terms(
                convertToInt(i)
              ).getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(l_total_tech_fees,
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_total_tech_fees", DoubleType,  false),
          StructField("l_is_deduction",    IntegerType, false),
          StructField("i",                 IntegerType, false),
          StructField("l_term_id",         IntegerType, false)
        )
      )
    )
  }

  def temp2053714_UDF = {
    udf(
      (
        _i:               Integer,
        _l_seller_fees:   Double,
        _l_is_deduction:  Integer,
        _l_pricing_terms: Seq[Row],
        _l_term_id:       Integer
      ) => {
        var i               = _i
        var l_seller_fees   = _l_seller_fees
        var l_is_deduction  = _l_is_deduction
        var l_pricing_terms = _l_pricing_terms.toArray
        var l_term_id       = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id != 90 && l_is_deduction == 1) {
            if (
              !_isnull(l_pricing_terms(convertToInt(i)).getAs[Double]("amount"))
            )
              l_seller_fees = l_seller_fees + l_pricing_terms(convertToInt(i))
                .getAs[Double]("amount")
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            l_seller_fees
        )
      },
      StructType(
        List(
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false),
          StructField("l_seller_fees",  DoubleType,  false)
        )
      )
    )
  }

  def temp2762257_UDF = {
    udf(
      (
        _l_data_costs:                Seq[Row],
        _l_member_sales_tax_rate_pct: Double,
        _i:                           Integer,
        _l_data_cost:                 Row,
        _data_costs:                  Seq[Row]
      ) => {
        var l_data_costs                = _l_data_costs.toArray
        var l_member_sales_tax_rate_pct = _l_member_sales_tax_rate_pct
        var i                           = _i
        var l_data_cost                 = _l_data_cost
        var data_costs                  = _data_costs.toArray
        while (compareTo(i, data_costs.length) < 0) {
          l_data_cost = data_costs(convertToInt(i))
          if (!_isnull(data_costs(convertToInt(i))))
            l_data_cost =
              updateIndexInRow(l_data_cost,
                               1,
                               l_data_cost.getAs[Double](
                                 "cost"
                               ) * (1.0d + l_member_sales_tax_rate_pct / 100.0d)
              )
          l_data_costs = Array.concat(l_data_costs, Array.fill(1)(l_data_cost))
          i = i + convertToInt(1)
        }
        Row(
          l_data_costs.map { x =>
            if (!_isnull(x))
              Row(convertToInt(x.get(0)),
                  x.get(1),
                  x.getAs[Seq[Integer]](2).toArray,
                  x.get(3)
              )
            else
              null
          }.toArray,
          convertToInt(i),
          if (!_isnull(l_data_cost))
            Row(convertToInt(l_data_cost.get(0)),
                l_data_cost.get(1),
                l_data_cost.getAs[Seq[Integer]](2).toArray,
                l_data_cost.get(3)
            )
          else
            null
        )
      },
      StructType(
        List(
          StructField(
            "l_data_costs",
            ArrayType(
              StructType(
                List(
                  StructField("data_member_id", IntegerType,            true),
                  StructField("cost",           DoubleType,             true),
                  StructField("used_segments",  ArrayType(IntegerType), false),
                  StructField("cost_pct",       DoubleType,             true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false),
          StructField(
            "l_data_cost",
            StructType(
              List(
                StructField("data_member_id", IntegerType,            true),
                StructField("cost",           DoubleType,             true),
                StructField("used_segments",  ArrayType(IntegerType), false),
                StructField("cost_pct",       DoubleType,             true)
              )
            ),
            false
          )
        )
      )
    )
  }

  def temp3471027_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                                        = _i
        var l_is_deduction                           = _l_is_deduction
        var l_pricing_terms                          = _l_pricing_terms.toArray
        var l_term_id                                = _l_term_id
        var l_is_curator_margin_media_cost_dependent = 0
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            )
              l_is_curator_margin_media_cost_dependent = convertToInt(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_curator_margin_media_cost_dependent),
            convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id)
        )
      },
      StructType(
        List(
          StructField("l_is_curator_margin_media_cost_dependent",
                      IntegerType,
                      false
          ),
          StructField("l_is_deduction", IntegerType, false),
          StructField("i",              IntegerType, false),
          StructField("l_term_id",      IntegerType, false)
        )
      )
    )
  }

  def temp4180587_UDF = {
    udf(
      (
        _l_term_id:       Integer,
        _l_pricing_terms: Seq[Row],
        _i:               Integer,
        _l_is_deduction:  Integer
      ) => {
        var i                     = _i
        var l_curator_margin_type = 0
        var l_is_deduction        = _l_is_deduction
        var l_pricing_terms       = _l_pricing_terms.toArray
        var l_term_id             = _l_term_id
        while (compareTo(i, l_pricing_terms.length) < 0) {
          l_term_id = 0
          l_is_deduction = 0
          if (
            !_isnull(l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"))
          )
            l_term_id = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id")
            )
          if (
            !_isnull(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          )
            l_is_deduction = convertToInt(
              l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction")
            )
          if (l_term_id == 95 && l_is_deduction == 1) {
            if (
              !_isnull(
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              )
            ) {
              if (
                l_pricing_terms(convertToInt(i)).getAs[Boolean](
                  "is_media_cost_dependent"
                ) == convertToBoolean(1)
              )
                l_curator_margin_type = 1
              else l_curator_margin_type = 2
            }
          }
          i = i + convertToInt(1)
        }
        Row(convertToInt(l_is_deduction),
            convertToInt(i),
            convertToInt(l_term_id),
            convertToInt(l_curator_margin_type)
        )
      },
      StructType(
        List(
          StructField("l_is_deduction",        IntegerType, false),
          StructField("i",                     IntegerType, false),
          StructField("l_term_id",             IntegerType, false),
          StructField("l_curator_margin_type", IntegerType, false)
        )
      )
    )
  }

  def temp6282792_UDF = {
    udf(
      (_in_pricing_terms: Seq[Row], _l_pricing_terms: Seq[Row], _i: Integer) =>
        {
          var l_pricing_terms  = _l_pricing_terms.toArray
          var i                = _i
          var in_pricing_terms = _in_pricing_terms.toArray
          while (compareTo(i, l_pricing_terms.length) < 0) {
            if (
              !_isnull(
                in_pricing_terms(convertToInt(i)).getAs[Double]("amount")
              ) && !_isnull(
                in_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent")
              ) && in_pricing_terms(convertToInt(i)).getAs[Boolean](
                "is_media_cost_dependent"
              ) == convertToBoolean(1)
            )
              l_pricing_terms(i) = Row(
                l_pricing_terms(convertToInt(i)).getAs[Integer]("term_id"),
                0,
                l_pricing_terms(convertToInt(i)).getAs[Double]("rate"),
                l_pricing_terms(convertToInt(i)).getAs[Boolean]("is_deduction"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Boolean]("is_media_cost_dependent"),
                l_pricing_terms(convertToInt(i))
                  .getAs[Integer]("data_member_id")
              )
            i = i + convertToInt(1)
          }
          Row(
            l_pricing_terms.map { x =>
              if (!_isnull(x))
                Row(convertToInt(x.get(0)),
                    x.get(1),
                    x.get(2),
                    x.get(3),
                    x.get(4),
                    convertToInt(x.get(5))
                )
              else
                null
            }.toArray,
            convertToInt(i)
          )
        },
      StructType(
        List(
          StructField(
            "l_pricing_terms",
            ArrayType(
              StructType(
                List(
                  StructField("term_id",                 IntegerType, true),
                  StructField("amount",                  DoubleType,  true),
                  StructField("rate",                    DoubleType,  true),
                  StructField("is_deduction",            BooleanType, true),
                  StructField("is_media_cost_dependent", BooleanType, true),
                  StructField("data_member_id",          IntegerType, true)
                )
              )
            ),
            false
          ),
          StructField("i", IntegerType, false)
        )
      )
    )
  }

  def temp6995147_UDF = {
    udf(
      (
        _id_type:   String,
        _pi_list:   Seq[Row],
        _i:         Integer,
        _l_id_type: Integer
      ) => {
        var pi        = Row(convertToInt(0), null)
        var l_id_type = _l_id_type
        var id_type   = _id_type
        var i         = _i
        var pi_list   = _pi_list.toArray
        while (compareTo(i, pi_list.length) < 0) {
          l_id_type = 0
          if (
            !_isnull(pi_list(convertToInt(i)).getAs[Integer]("identity_type"))
          )
            l_id_type = convertToInt(
              pi_list(convertToInt(i)).getAs[Integer]("identity_type")
            )
          if (l_id_type.toString == id_type)
            pi = pi_list(convertToInt(i))
          i = i + convertToInt(1)
        }
        Row(if (!_isnull(pi))
              Row(convertToInt(pi.get(0)), pi.get(1).toString)
            else
              null,
            convertToInt(l_id_type),
            convertToInt(i)
        )
      },
      StructType(
        List(
          StructField("pi",
                      StructType(
                        List(StructField("identity_type",  IntegerType, false),
                             StructField("identity_value", StringType,  true)
                        )
                      ),
                      false
          ),
          StructField("l_id_type", IntegerType, false),
          StructField("i",         IntegerType, false)
        )
      )
    )
  }

  def rollup1615_UDF = {
    udf(
      (_view_result: Seq[Integer], _temp_view_result: Integer) => {
        var view_result      = _view_result
        var temp_view_result = _temp_view_result
        var i                = 0
        while (i < view_result.length) {
          temp_view_result =
            if (
              (if (temp_view_result == null)
                 0
               else
                 temp_view_result) == (if (view_result(i) == null)
                                         0
                                       else
                                         view_result(i)) || (if (
                                                               view_result(
                                                                 i
                                                               ) == null
                                                             )
                                                               0
                                                             else
                                                               view_result(
                                                                 i
                                                               )) == 0 || (if (
                                                                             temp_view_result == null
                                                                           )
                                                                             0
                                                                           else
                                                                             temp_view_result) == 1
            ) {
              if (temp_view_result == null)
                0
              else
                temp_view_result
            } else {
              if (
                (if (temp_view_result == null)
                   0
                 else
                   temp_view_result) == 0 || (if (view_result(i) == null)
                                                0
                                              else
                                                view_result(i)) == 1
              ) {
                if (view_result(i) == null)
                  0
                else
                  view_result(i)
              } else
                3
            }
          i = i + 1
        }
        temp_view_result
      },
      IntegerType
    )
  }

  def rollup1621_UDF = {
    udf(
      (
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var i                          = 0
        while (i < viewdef_definition_id.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_definition_id
      },
      IntegerType
    )
  }

  def rollup1627_UDF = {
    udf(
      (
        _viewdef_view_result:        Seq[Integer],
        _viewdef_definition_id:      Seq[Integer],
        _temp_viewdef_view_result:   Integer,
        _temp_viewdef_definition_id: Integer
      ) => {
        var viewdef_view_result        = _viewdef_view_result
        var viewdef_definition_id      = _viewdef_definition_id
        var temp_viewdef_definition_id = _temp_viewdef_definition_id
        var temp_viewdef_view_result   = _temp_viewdef_view_result
        var i                          = 0
        while (i < viewdef_view_result.length) {
          temp_viewdef_definition_id =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_definition_id == null)
                0
              else
                temp_viewdef_definition_id
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_definition_id(i) == null)
                  0
                else
                  viewdef_definition_id(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (temp_viewdef_definition_id == null)
                    0
                  else
                    temp_viewdef_definition_id
                } else
                  0
              }
            }
          temp_viewdef_view_result =
            if (
              compareTo(if (temp_viewdef_definition_id == null)
                          0
                        else
                          temp_viewdef_definition_id,
                        if (viewdef_definition_id(i) == null)
                          0
                        else
                          viewdef_definition_id(i)
              ) > 0
            ) {
              if (temp_viewdef_view_result == null)
                0
              else
                temp_viewdef_view_result
            } else {
              if (
                compareTo(if (temp_viewdef_definition_id == null)
                            0
                          else
                            temp_viewdef_definition_id,
                          if (viewdef_definition_id(i) == null)
                            0
                          else
                            viewdef_definition_id(i)
                ) < 0
              ) {
                if (viewdef_view_result(i) == null)
                  0
                else
                  viewdef_view_result(i)
              } else {
                if (
                  compareTo(if (temp_viewdef_definition_id == null)
                              0
                            else
                              temp_viewdef_definition_id,
                            0
                  ) > 0
                ) {
                  if (
                    (if (temp_viewdef_view_result == null)
                       0
                     else
                       temp_viewdef_view_result) == (if (
                                                       viewdef_view_result(
                                                         i
                                                       ) == null
                                                     )
                                                       0
                                                     else
                                                       viewdef_view_result(
                                                         i
                                                       )) || (if (
                                                                viewdef_view_result(
                                                                  i
                                                                ) == null
                                                              )
                                                                0
                                                              else
                                                                viewdef_view_result(
                                                                  i
                                                                )) == 0 || (if (
                                                                              temp_viewdef_view_result == null
                                                                            )
                                                                              0
                                                                            else
                                                                              temp_viewdef_view_result) == 1
                  ) {
                    if (temp_viewdef_view_result == null)
                      0
                    else
                      temp_viewdef_view_result
                  } else {
                    if (
                      (if (temp_viewdef_view_result == null)
                         0
                       else
                         temp_viewdef_view_result) == 0 || (if (
                                                              viewdef_view_result(
                                                                i
                                                              ) == null
                                                            )
                                                              0
                                                            else
                                                              viewdef_view_result(
                                                                i
                                                              )) == 1
                    ) {
                      if (viewdef_view_result(i) == null)
                        0
                      else
                        viewdef_view_result(i)
                    } else
                      3
                  }
                } else
                  0
              }
            }
          i = i + 1
        }
        temp_viewdef_view_result
      },
      IntegerType
    )
  }

  def f_create_agg_dw_impressions_virtual_log_dw_bid_10949800 = {
    udf(
      {
        (
          _virtual_log_dw_bid:                 Row,
          _f_is_buy_side:                      Int,
          log_dw_bid:                          Row,
          f_is_error_imp:                      Int,
          is_dw:                               Int,
          imp_type:                            Int,
          f_is_default_or_error_imp:           Int,
          log_impbus_impressions_payment_type: Int,
          seller_trx_event_id:                 Int
        ) =>
          var virtual_log_dw_bid = _virtual_log_dw_bid
          if (_f_is_buy_side == 0) {
            if (
              log_dw_bid == null && (f_is_error_imp == 1 || is_dw == 1 && (imp_type == 2 || imp_type == 4))
            ) {
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 1,  1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 0,  1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 48, 1)
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid, 59, "--")
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid,                    46, "\\\\N")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 62, 0)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 10, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 11, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 51, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 52, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 8,  -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 9,  -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 53, -2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 54, -2)
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid, 18, "---")
              virtual_log_dw_bid =
                updateIndexInRow(virtual_log_dw_bid,                    24, "USD")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 25, 1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 55, "u")
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 23, 1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 20, 2)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 27, -1)
              virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid, 28, -1)
            } else {
              if (f_is_default_or_error_imp == 1) {
                if (log_dw_bid != null) virtual_log_dw_bid = log_dw_bid
                if (f_is_default_or_error_imp == 1) {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 10, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 11, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 51, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 52, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 8, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 9, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 53, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 54, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 4, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 6, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 5, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 7, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 23, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 12, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 72, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 73, 0)
                }
                if (f_is_error_imp == 1) {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 24, "USD")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 25, 1.0d)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 55, "u")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 56, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 19, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 20, 2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 21, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 22, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 27, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 28, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 18, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 7, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 26, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 43, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 16, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 23, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 31, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 32, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 14, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 17, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 2, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 33, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 34, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 29, 0)
                }
              } else {
                if (imp_type == 9 || imp_type == 2 || imp_type == 5)
                  virtual_log_dw_bid = log_dw_bid
                else {
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 1, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 0, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 48, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 10, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 11, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 51, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 52, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 8, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 9, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 53, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 54, -1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 57, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 58, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 59, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 24, "USD")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 25, 1)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 18, "---")
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 66, 2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 41, 0)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid,
                                     37,
                                     log_impbus_impressions_payment_type
                    )
                  virtual_log_dw_bid = updateIndexInRow(virtual_log_dw_bid,
                                                        62,
                                                        seller_trx_event_id
                  )
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 28, -2)
                  virtual_log_dw_bid =
                    updateIndexInRow(virtual_log_dw_bid, 27, -2)
                }
              }
            }
          }
          virtual_log_dw_bid
      },
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
          StructField(
            "revenue_info",
            StructType(
              Array(
                StructField("total_partner_fees_microcents", LongType,   true),
                StructField("booked_revenue_dollars",        DoubleType, true),
                StructField("booked_revenue_adv_curr",       DoubleType, true),
                StructField("total_data_costs_microcents",   LongType,   true),
                StructField("total_profit_microcents",       LongType,   true),
                StructField("total_segment_data_costs_microcents",
                            LongType,
                            true
                ),
                StructField("total_feature_costs_microcents", LongType, true)
              )
            ),
            true
          ),
          StructField("use_revenue_info",              BooleanType, true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType,  true),
          StructField("has_crossdevice_reach_extension",   BooleanType, true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType,  true),
          StructField("bpp_multiplier",                    DoubleType,  true),
          StructField("bpp_offset",                        DoubleType,  true),
          StructField("bid_modifier",                      DoubleType,  true),
          StructField("payment_value_microcents",          LongType,    true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      BooleanType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
        )
      )
    )
  }

}

object PipelineInitCode extends Serializable
