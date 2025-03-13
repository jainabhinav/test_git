from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Cleanse_84(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "AIN Ext__ Lab__ Net Savings Stage" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN Ext__ Lab__ Net Savings Stage' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN Ext__ Lab__ Net Savings Stage"].dataType, StringType):
            transformed_columns = [col("AIN Ext__ Lab__ Net Savings Stage")]
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN Ext__ Lab__ Net Savings Stage"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns = [col("AIN Ext__ Lab__ Net Savings Stage")]
        else:
            transformed_columns = [col("AIN Ext__ Lab__ Net Savings Stage")]

    # Check if column exists after null operations
    if "GSS Enterprise Savings" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS Enterprise Savings' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS Enterprise Savings"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS Enterprise Savings"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS Enterprise Savings"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS Enterprise Savings"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS Enterprise Savings"))

    # Check if column exists after null operations
    if "In__Scope" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'In__Scope' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["In__Scope"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("In__Scope"))
        elif isinstance(
            in0.na.drop(how = "all").schema["In__Scope"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("In__Scope"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("In__Scope"))

    # Check if column exists after null operations
    if "scenario name" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'scenario name' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["scenario name"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("scenario name"))
        elif isinstance(
            in0.na.drop(how = "all").schema["scenario name"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("scenario name"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("scenario name"))

    # Check if column exists after null operations
    if "year" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'year' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["year"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("year"))
        elif isinstance(
            in0.na.drop(how = "all").schema["year"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("year"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("year"))

    # Check if column exists after null operations
    if "Category" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'Category' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Category"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Category"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Category"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Category"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Category"))

    # Check if column exists after null operations
    if "Sum_Adjusted_Sum_USDAFX" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Sum_Adjusted_Sum_USDAFX' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_USDAFX"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Sum_Adjusted_Sum_USDAFX"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_USDAFX"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Sum_Adjusted_Sum_USDAFX"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Sum_Adjusted_Sum_USDAFX"))

    # Check if column exists after null operations
    if "Sum_Adjusted_Sum_LCLCFX" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Sum_Adjusted_Sum_LCLCFX' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_LCLCFX"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Sum_Adjusted_Sum_LCLCFX"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_LCLCFX"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Sum_Adjusted_Sum_LCLCFX"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Sum_Adjusted_Sum_LCLCFX"))

    # Check if column exists after null operations
    if "Sum_Adjusted_Sum_LCL" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Sum_Adjusted_Sum_LCL' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_LCL"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Sum_Adjusted_Sum_LCL"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Sum_Adjusted_Sum_LCL"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Sum_Adjusted_Sum_LCL"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Sum_Adjusted_Sum_LCL"))

    # Check if column exists after null operations
    if "evp" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'evp' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["evp"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("evp"))
        elif isinstance(
            in0.na.drop(how = "all").schema["evp"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("evp"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("evp"))

    # Check if column exists after null operations
    if "cost center number" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'cost center number' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["cost center number"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("cost center number"))
        elif isinstance(
            in0.na.drop(how = "all").schema["cost center number"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("cost center number"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("cost center number"))

    # Check if column exists after null operations
    if "hlmc" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'hlmc' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["hlmc"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("hlmc"))
        elif isinstance(
            in0.na.drop(how = "all").schema["hlmc"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("hlmc"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("hlmc"))

    # Check if column exists after null operations
    if "mc" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'mc' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["mc"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("mc"))
        elif isinstance(
            in0.na.drop(how = "all").schema["mc"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("mc"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("mc"))

    # Check if column exists after null operations
    if "cost center" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'cost center' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["cost center"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("cost center"))
        elif isinstance(
            in0.na.drop(how = "all").schema["cost center"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("cost center"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("cost center"))

    # Check if column exists after null operations
    if "quarter" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'quarter' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["quarter"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("quarter"))
        elif isinstance(
            in0.na.drop(how = "all").schema["quarter"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("quarter"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("quarter"))

    # Check if column exists after null operations
    if "site" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'site' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["site"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("site"))
        elif isinstance(
            in0.na.drop(how = "all").schema["site"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("site"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("site"))

    # Check if column exists after null operations
    if "Non__Opex Adjustments" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Non__Opex Adjustments' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Non__Opex Adjustments"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Non__Opex Adjustments"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Non__Opex Adjustments"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Non__Opex Adjustments"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Non__Opex Adjustments"))

    # Check if column exists after null operations
    if "Planning Account" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Planning Account' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Planning Account"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Planning Account"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Planning Account"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Planning Account"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Planning Account"))

    # Check if column exists after null operations
    if "Regrouped Level 4" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Regrouped Level 4' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Regrouped Level 4"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Regrouped Level 4"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Regrouped Level 4"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Regrouped Level 4"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Regrouped Level 4"))

    # Check if column exists after null operations
    if "Company Code" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Company Code' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Company Code"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Company Code"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Company Code"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Company Code"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Company Code"))

    # Check if column exists after null operations
    if "Company Description" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Company Description' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Company Description"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Company Description"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Company Description"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Company Description"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Company Description"))

    # Check if column exists after null operations
    if "Planning Account Description" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Planning Account Description' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Planning Account Description"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Planning Account Description"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Planning Account Description"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Planning Account Description"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Planning Account Description"))

    # Check if column exists after null operations
    if "Hyperion Category" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Hyperion Category' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Hyperion Category"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Hyperion Category"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Hyperion Category"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Hyperion Category"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Hyperion Category"))

    # Check if column exists after null operations
    if "Hyperion Category Code" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Hyperion Category Code' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Hyperion Category Code"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Hyperion Category Code"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Hyperion Category Code"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Hyperion Category Code"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Hyperion Category Code"))

    # Check if column exists after null operations
    if "CTS V3" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'CTS V3' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["CTS V3"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("CTS V3"))
        elif isinstance(
            in0.na.drop(how = "all").schema["CTS V3"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("CTS V3"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("CTS V3"))

    # Check if column exists after null operations
    if "gl account" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'gl account' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["gl account"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("gl account"))
        elif isinstance(
            in0.na.drop(how = "all").schema["gl account"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("gl account"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("gl account"))

    # Check if column exists after null operations
    if "sub mc" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'sub mc' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["sub mc"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("sub mc"))
        elif isinstance(
            in0.na.drop(how = "all").schema["sub mc"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("sub mc"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("sub mc"))

    # Check if column exists after null operations
    if "material group" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'material group' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["material group"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("material group"))
        elif isinstance(
            in0.na.drop(how = "all").schema["material group"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("material group"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("material group"))

    # Check if column exists after null operations
    if "material group description" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'material group description' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["material group description"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("material group description"))
        elif isinstance(
            in0.na.drop(how = "all").schema["material group description"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("material group description"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("material group description"))

    # Check if column exists after null operations
    if "gl account number" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'gl account number' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["gl account number"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("gl account number"))
        elif isinstance(
            in0.na.drop(how = "all").schema["gl account number"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("gl account number"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("gl account number"))

    # Check if column exists after null operations
    if "OSE Labor V2" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'OSE Labor V2' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["OSE Labor V2"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("OSE Labor V2"))
        elif isinstance(
            in0.na.drop(how = "all").schema["OSE Labor V2"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("OSE Labor V2"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("OSE Labor V2"))

    # Check if column exists after null operations
    if "ASHB" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'ASHB' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["ASHB"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("ASHB"))
        elif isinstance(
            in0.na.drop(how = "all").schema["ASHB"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("ASHB"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("ASHB"))

    # Check if column exists after null operations
    if "version" not in in0.na.drop(how = "all").columns:
        print("Warning: Column 'version' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["version"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("version"))
        elif isinstance(
            in0.na.drop(how = "all").schema["version"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("version"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("version"))

    # Check if column exists after null operations
    if "scenario type" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'scenario type' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["scenario type"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("scenario type"))
        elif isinstance(
            in0.na.drop(how = "all").schema["scenario type"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("scenario type"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("scenario type"))

    # Check if column exists after null operations
    if "Cash Spend" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Cash Spend' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Cash Spend"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Cash Spend"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Cash Spend"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Cash Spend"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Cash Spend"))

    # Check if column exists after null operations
    if "Planning Account Actual" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Planning Account Actual' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Planning Account Actual"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Planning Account Actual"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Planning Account Actual"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Planning Account Actual"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Planning Account Actual"))

    # Check if column exists after null operations
    if "Planning Account Description Actual" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Planning Account Description Actual' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Planning Account Description Actual"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Planning Account Description Actual"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Planning Account Description Actual"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Planning Account Description Actual"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Planning Account Description Actual"))

    # Check if column exists after null operations
    if "GSS BAU Savings" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS BAU Savings' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS BAU Savings"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS BAU Savings"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS BAU Savings"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS BAU Savings"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS BAU Savings"))

    # Check if column exists after null operations
    if "GSS Incremental Low Savings" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS Incremental Low Savings' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS Incremental Low Savings"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS Incremental Low Savings"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS Incremental Low Savings"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS Incremental Low Savings"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS Incremental Low Savings"))

    # Check if column exists after null operations
    if "GSS Incremental High Savings" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS Incremental High Savings' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS Incremental High Savings"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS Incremental High Savings"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS Incremental High Savings"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS Incremental High Savings"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS Incremental High Savings"))

    # Check if column exists after null operations
    if "AIN_Resource_Cost" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN_Resource_Cost' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN_Resource_Cost"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN_Resource_Cost"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN_Resource_Cost"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN_Resource_Cost"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN_Resource_Cost"))

    # Check if column exists after null operations
    if "AIN_Savings" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN_Savings' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN_Savings"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN_Savings"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN_Savings"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN_Savings"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN_Savings"))

    # Check if column exists after null operations
    if "AIN EW_Reduction" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN EW_Reduction' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN EW_Reduction"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN EW_Reduction"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN EW_Reduction"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN EW_Reduction"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN EW_Reduction"))

    # Check if column exists after null operations
    if "GSS BAU Savings (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS BAU Savings (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS BAU Savings (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS BAU Savings (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS BAU Savings (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS BAU Savings (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS BAU Savings (Realised)"))

    # Check if column exists after null operations
    if "GSS Incremental Low Savings (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS Incremental Low Savings (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS Incremental Low Savings (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS Incremental Low Savings (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS Incremental Low Savings (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS Incremental Low Savings (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS Incremental Low Savings (Realised)"))

    # Check if column exists after null operations
    if "GSS Incremental High Savings (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'GSS Incremental High Savings (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["GSS Incremental High Savings (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("GSS Incremental High Savings (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["GSS Incremental High Savings (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("GSS Incremental High Savings (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("GSS Incremental High Savings (Realised)"))

    # Check if column exists after null operations
    if "AIN_Resource_Cost (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN_Resource_Cost (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN_Resource_Cost (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN_Resource_Cost (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN_Resource_Cost (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN_Resource_Cost (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN_Resource_Cost (Realised)"))

    # Check if column exists after null operations
    if "AIN_Savings (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN_Savings (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN_Savings (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN_Savings (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN_Savings (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN_Savings (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN_Savings (Realised)"))

    # Check if column exists after null operations
    if "AIN EW_Reduction (Realised)" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'AIN EW_Reduction (Realised)' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["AIN EW_Reduction (Realised)"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("AIN EW_Reduction (Realised)"))
        elif isinstance(
            in0.na.drop(how = "all").schema["AIN EW_Reduction (Realised)"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("AIN EW_Reduction (Realised)"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("AIN EW_Reduction (Realised)"))

    # Check if column exists after null operations
    if "tableau display category" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'tableau display category' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["tableau display category"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("tableau display category"))
        elif isinstance(
            in0.na.drop(how = "all").schema["tableau display category"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("tableau display category"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("tableau display category"))

    # Check if column exists after null operations
    if "Company Group" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'Company Group' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["Company Group"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Company Group"))
        elif isinstance(
            in0.na.drop(how = "all").schema["Company Group"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Company Group"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Company Group"))

    # Check if column exists after null operations
    if "In__Scope Final" not in in0.na.drop(how = "all").columns:
        print(
            "Warning: Column 'In__Scope Final' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(in0.na.drop(how = "all").schema["In__Scope Final"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(col("In__Scope Final"))
        elif isinstance(
            in0.na.drop(how = "all").schema["In__Scope Final"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("In__Scope Final"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("In__Scope Final"))

    return in0.na\
        .drop(how = "all")\
        .select(*[
        col(c)
        for c in in0.na.drop(how = "all").columns
        if (
        c
        not in ["AIN Ext__ Lab__ Net Savings Stage",  "GSS Enterprise Savings",  "In__Scope",  "scenario name",  "year",  "Category",            "Sum_Adjusted_Sum_USDAFX",  "Sum_Adjusted_Sum_LCLCFX",  "Sum_Adjusted_Sum_LCL",  "evp",            "cost center number",  "hlmc",  "mc",  "cost center",  "quarter",  "site",  "Non__Opex Adjustments",            "Planning Account",  "Regrouped Level 4",  "Company Code",  "Company Description",            "Planning Account Description",  "Hyperion Category",  "Hyperion Category Code",  "CTS V3",  "gl account",            "sub mc",  "material group",  "material group description",  "gl account number",  "OSE Labor V2",  "ASHB",            "version",  "scenario type",  "Cash Spend",  "Planning Account Actual",            "Planning Account Description Actual",  "GSS BAU Savings",  "GSS Incremental Low Savings",            "GSS Incremental High Savings",  "AIN_Resource_Cost",  "AIN_Savings",  "AIN EW_Reduction",            "GSS BAU Savings (Realised)",  "GSS Incremental Low Savings (Realised)",            "GSS Incremental High Savings (Realised)",  "AIN_Resource_Cost (Realised)",  "AIN_Savings (Realised)",            "AIN EW_Reduction (Realised)",  "tableau display category",  "Company Group",  "In__Scope Final"]
    )
    ], *transformed_columns)
