# Convert_Athena_code_into_Pyspark_in_Databricks
Convert_Athena_code_into_Pyspark_in_Databricks


def pull_data() -> pd.DataFrame:
    """
    Executes SQL query against Krakencore to return eacs.
    """
    # Gets the Elec meter point details
    sql_file = SQL_DIR + 'eac.sql'

    dtypes = {
        'meter_point_id':       'Int64',
        'ssc':                  'string',
        'profile_class':        'Int64',
        'tpr':                  'string',
        'eac_id':               'Int64',
        'eac':                  'float',
        'effective_from_date':  'datetime64[ns]',
        'effective_to_date':    'datetime64[ns]',
    }

    # Execute SQL query in parallel chunks
    df = u.execute_sql_script_parallel(
        base_table='properties_electricitymeterpoint',
        sql_filepath=sql_file,
        conn_str=CONN_STR_KRAKENCORE,
        dtypes=dtypes,
        num_dask_workers=NUM_DASK_THREADS_SQL,
    )

    logger.info(f'SQL query executed, {len(df)} rows returned')

    return df


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process eacs.
    """
    # Filter on columns of interest
    keep_cols = [
        'meter_point_id',
        'ssc',
        'profile_class',
        'tpr',
        'eac_id',
        'eac',
        'effective_from_date',
        'effective_to_date',
    ]
    df = df[keep_cols]

    # Fill meterpoint configuration parameters (we have no history
    # pre-ssd so we must fill the gaps)

    df_filled = df.sort_values(['meter_point_id', 'effective_from_date'])[['meter_point_id', 'ssc', 'profile_class']].copy()

    df_filled.update(df_filled.groupby('meter_point_id').ffill())

    df_filled.update(df_filled.groupby('meter_point_id').bfill())

    df[df_filled.columns] = df[df_filled.columns].fillna(df_filled)a

    
    logger.info('Data processed.')

    return df


def write_data(df: pd.DataFrame) -> None:
    """
    Write data to parquet files.
    """
    # Convert meter_point_id to partition_id and set as index
    df = u.index_to_partition_id(
        df=df,
        dataset_class=EAC,
    )

    # Save data to parquet files using DASK
    ds = EAC()
    logger.info(rich.inspect(ds))
    ds.write(
        df=df,
        num_dask_workers=NUM_DASK_WORKERS,
    )

    logger.info(f'Parquet files written to {ds.parquet_directory}.')


def calculate_mean_eacs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the mean EAC per ssc
    """
    # Calculate mean EAC per ssc and filter on values based on > MIN_N_SSC_EAC mpans
    ssc_mean_eac = (
        df
        .dropna(subset=['ssc'])
        .groupby(['ssc'])['eac']
        .agg(['mean', 'count'])
    )

    # Only keep rows where we have more than 10 counts
    ssc_mean_eac = ssc_mean_eac[ssc_mean_eac['count'] > 10]['mean'].round()

    # Rename column
    ssc_mean_eac_df = ssc_mean_eac.rename('mean_eac').reset_index()

    logger.info('Mean EACs calculated.')

    return ssc_mean_eac_df
