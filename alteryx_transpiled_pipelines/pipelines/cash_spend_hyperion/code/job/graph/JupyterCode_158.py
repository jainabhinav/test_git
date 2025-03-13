from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_158(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame):
    #################################
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    #################################
    #################################
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    #################################
    import pandas as pd
    import numpy as np
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
    merged_df['mc'] = merged_df['mc'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['regrouped level 4'] = merged_df['regrouped level 4'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['gl account'] = merged_df['gl account'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['gl account number'] = merged_df['gl account number'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['sub mc'] = merged_df['sub mc'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['material group'] = merged_df['material group'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['planning sku'] = merged_df['planning sku'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['product description'] = merged_df['product description'].replace(np.nan, 'NOT AVAILABLE')
    merged_df['scenario type'] = merged_df['scenario type'].replace(np.nan, 'NOT AVAILABLE')
    # Drop the 'pa account' and 'Planning Account' columns since we want to roll up by Category
    #merged_df.drop(columns=['pa account', 'Planning Account'], inplace=True)
    # Group by year, version, scenario name, cost center number, evp, and Category, and sum the adjusted columns
    grouped_df = merged_df\
                     .groupby(
                       ['year', 'version', 'scenario name', 'cost center number', 'evp', 'Category', 'hlmc', 'mc',
                        'cost center', 'quarter', 'site', 'pa account', 'regrouped level 4',
                        'hyperion category', 'hyperion category code', 'pa account desc',
                        'company', 'company code', 'scenario type', 'gl account',
                        'gl account number', 'sub mc', 'material group', 'planning sku',
                        'product description']
                     )\
                     .agg({
'Adjusted_Sum_USDAFX' : 'sum', 'Adjusted_Sum_LCLCFX' : 'sum', 'Adjusted_Sum_LCL' : 'sum'})\
                     .reset_index()
    # Output the final rolled-up result back to Alteryx (to the first output anchor #1)
    Alteryx.write(grouped_df, 1)
    # Now, to find PA accounts in df1 that do not have a corresponding match in df2:
    # Perform a left join and filter for rows where 'Planning Account' is NaN
    non_joined_accounts = pd.merge(df1, df2, how = 'left', left_on = 'pa account', right_on = 'Planning Account')
    non_joined_accounts = non_joined_accounts[non_joined_accounts['Planning Account'].isna()]
    # Select only the 'pa account' column, as it represents the unmatched accounts
    non_joined_accounts = non_joined_accounts[['pa account']].drop_duplicates()
    print(non_joined_accounts.head())
    # Output the non-joined PA accounts to the second Alteryx output anchor (#2)
    Alteryx.write(non_joined_accounts, 2)

    return (out0, out1)
