from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_109(spark: SparkSession, in0: DataFrame) -> DataFrame:
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    import pandas as pd
    from ayx import Alteryx
    # Read Dataset 1 from the first Alteryx input anchor (#1)
    df1 = Alteryx.read(
        "#1"
    ) # This will be the main dataset (Input Data)
    # Read Dataset 2 (GSS Mega Category) from the second Alteryx input anchor (#2)
    df2 = Alteryx.read(
        "#2"
    ) # This will be the GSS Mega Category dataset
    # Perform an inner join on 'pa account' from df1 and 'Planning Account' from df2
    merged_df = pd.merge(df1, df2, how = 'inner', left_on = 'pa account', right_on = 'Planning Account')
    # Multiply financial columns by Percent_of_Account to distribute the amounts correctly
    merged_df['Adjusted_Sum_USDAFX'] = merged_df['Sum_USDAFX'] * (merged_df['Percent_of_Account'] / 100)
    merged_df['Adjusted_Sum_LCLCFX'] = merged_df['Sum_LCLCFX'] * (merged_df['Percent_of_Account'] / 100)
    merged_df['Adjusted_Sum_LCL'] = merged_df['Sum_LCL'] * (merged_df['Percent_of_Account'] / 100)
    # Drop the 'pa account' column since we want to roll up by Category
    merged_df.drop(columns = ['pa account', 'Planning Account'], inplace = True)
    # Group by year, version, scenario name, and Category, and sum the adjusted columns
    grouped_df = merged_df\
                     .groupby(['year', 'version', 'scenario name', 'Category'])\
                     .agg({
'Adjusted_Sum_USDAFX' : 'sum', 'Adjusted_Sum_LCLCFX' : 'sum', 'Adjusted_Sum_LCL' : 'sum'})\
                     .reset_index()
    # Output the final rolled-up result back to Alteryx (to the first output anchor #1)
    Alteryx.write(grouped_df, 1)

    return out0
