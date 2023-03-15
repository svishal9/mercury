import logging
import datetime
from pytz import timezone
from pyspark.sql import SparkSession

__version__ = "0.1.0"
logger = logging.getLogger('Generate input for item categorization model')
logger.setLevel(logging.INFO)
MAX_RECORDS_PARQUET_FILES = 1000000


def create_spark_context(gdp_environment, etl_date):
    """
    Create spark_session context and session
    :param gdp_environment: environment dev/qa/prod
    :param etl_date: date of etl
    :return: spark_session session and spark_session context
    """
    # conf = SparkConf().setAppName("Python Spark Item Categorization_{}_{}".
    #                               format(gdp_environment, etl_date)) \
    #     .set("spark.sql.parquet.filterPushdown", "true") \
    #     .set("spark.sql.parquet.cacheMetadata", "true") \
    #     .set("spark.shuffle.consolidateFiles", "true") \
    #     .set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
    #     .set("spark.sql.warehouse.dir", "hdfs:///user/spark/warehouse")
    # spark_context = SparkContext(conf=conf)
    # spark_session = SparkSession(spark_context).builder.enableHiveSupport().getOrCreate()
    spark_session = SparkSession \
        .builder \
        .appName(f"Python Spark Pipeline_{gdp_environment}_{etl_date}") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.cacheMetadata", "true") \
        .getOrCreate()
    return spark_session


def read_register_spark_view(spark_session, view_name, file_path, csv_file=True):
    """
    Function to read parquet and register as view
    :param remove_gdp_cols: Boolean value to indicate if gdp columns are to be dropped
    :param csv_file: Boolean value indicating if file is csv or not
    :param view_name: View name to register
    :param file_path: Parquet path to read
    :param spark_session: Spark session
    """
    if not csv_file:
        log_message(f"Reading parquet for {view_name} view")
        df_view = spark_session.read.parquet(file_path)
    else:
        log_message(f"Reading csv for {view_name} view")
        df_view = spark_session.read.csv(file_path, header='true')
    log_message(f"Count of rows for {view_name}: {df_view.count()}")
    log_message(f"Creating {view_name} view")
    df_view.createOrReplaceTempView(view_name)


def execute_sql_and_register_spark_view(spark_session, sql_stmt, view_name):
    """
    Function to execute spark sql and register as view
    :param view_name: View name to register
    :param sql_stmt: Spark SQL
    :param spark_session: Spark session
    """
    df_sql_exec = spark_session.sql(sql_stmt)
    log_message(f"Count of rows for {view_name}: {df_sql_exec.count()}")
    df_sql_exec.createOrReplaceTempView(view_name)


def log_message(message, log_name='ic_app_log'):
    """
    Function to log message
    :param log_name: name of log to generate
    :param message: message to log
    """
    print(datetime.datetime.now(timezone('Australia/Sydney')).strftime(
        "%Y-%m-%d %H:%M:%S") + f": {log_name} : {message}")


def get_sql_stmt_df(table_name):
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select * from {table_name}
    """


def get_sql_stmt_d_customer_master():
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select cast(CustomerID as bigint) customer_id,
cast(RetailerID as bigint) retailer_id,
CreatedOn profile_created_on,
LastUpdated profile_updated_on,
current_date effective_date,
'9999-12-31' expiry_date,
current_date etl_date
from customer
    """


def get_sql_stmt_d_retailer_master():
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select cast(RetailerID as bigint) retailer_id,
cast(RetailerName as bigint) retailer_name,
current_date effective_date,
'9999-12-31' expiry_date,
current_date etl_date
from retailer
    """


def get_sql_stmt_f_transaction_sales():
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select uuid() transaction_id,
cast(CustomerID as bigint) customer_id,
cast(RetailerID as bigint) retailer_id,
DepositOnUTC order_date,
PurchasePrice purchase_price,
weekofyear(to_date(substr(LastUpdated,1,10),'dd/MM/yyyy')) week_number,
1 is_active_customer,
Deposit amount_paid,
LastUpdated last_updated,
current_date etl_date
from transactions
where RefundedOn = 'NULL'
    """


def get_sql_stmt_f_transaction_refunds():
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select uuid() transaction_id,
cast(CustomerID as bigint) customer_id,
cast(RetailerID as bigint) retailer_id,
RefundedOn refund_date,
'N/A' refund_reason_code,
weekofyear(to_date(substr(LastUpdated,1,10),'dd/MM/yyyy')) week_number,
1 was_active_customer,
Deposit refund_amount,
LastUpdated last_updated,
current_date etl_date
from transactions
where RefundedOn != 'NULL'
    """


def write_results_to_disk(spark_session, table_name, output_path):
    df_res = spark_session.sql(get_sql_stmt_df(table_name))
    log_message(df_res.printSchema())
    df_res.repartition("etl_date").write.mode("overwrite") \
        .option("maxRecordsPerFile", MAX_RECORDS_PARQUET_FILES) \
        .parquet(output_path)
    log_message("Finished writing to disk")
    return 0


def process_table(spark_session, sql_text, table_name, output_path):
    execute_sql_and_register_spark_view(spark_session, sql_text, table_name)
    write_results_to_disk(spark_session, table_name, output_path)


def generate_analysis_data(spark_session, path_merchant_table, path_transaction_table, path_customer_table,
                           etl_date, base_path):
    """
    generate input for the model
    :param base_path:
    :param etl_date:
    :param path_customer_table:
    :param path_transaction_table:
    :param spark_session: spark session
    :param path_merchant_table: location of merchant table to get trading name
    :return: 0 if successful
    """
    log_message("Started generating input for retailer etl")
    log_message(f"etl_date: {etl_date}")
    read_register_spark_view(spark_session, 'retailer', path_merchant_table)
    read_register_spark_view(spark_session, 'transactions', path_transaction_table)
    read_register_spark_view(spark_session, 'customer', path_customer_table)
    process_table(spark_session, get_sql_stmt_d_customer_master(), 'd_customer_master', base_path + "/customer")
    process_table(spark_session, get_sql_stmt_d_retailer_master(), 'd_retailer_master', base_path + "/retailer")
    process_table(spark_session, get_sql_stmt_f_transaction_sales(), 'f_transaction_sales', base_path + "/sales")
    process_table(spark_session, get_sql_stmt_f_transaction_refunds(), 'f_transaction_refunds', base_path + "/refunds")
    return 0