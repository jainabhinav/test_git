from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JupyterCode_141(spark: SparkSession, in0: DataFrame) -> DataFrame:
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    #################################
    # List all non-standard packages to be imported by your
    # script here (only missing packages will be installed)
    from ayx import Package
    #Package.installPackages(['pandas','numpy'])
    #################################
    # Import necessary libraries
    import pandas as pd
    from ayx import Alteryx
    # Read in the data from the Alteryx workflow input
    df = Alteryx.read(
        "#1"
    ) # This assumes your input data is connected to the first input anchor
    # Count the number of pa account per Hyp Cat
    df['pa_account_count'] = df.groupby('Hyp Cat')['pa account'].transform('count')
    # Calculating the percentage of spend per Hyp Cat for each pa account
    df['percent_of_spend'] = 100 / df['pa_account_count']
    # Dropping the intermediate column 'pa_account_count'
    df = df.drop(columns = ['pa_account_count'])
    # Write the resulting DataFrame back to the Alteryx workflow output
    Alteryx.write(df, 1) # This writes the output to the first output anchor

    return out0
