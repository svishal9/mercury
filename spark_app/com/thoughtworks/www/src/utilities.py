import logging
import os
import shutil

from faker import Faker
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import udf, row_number, col, lit
from pyspark.sql.types import StringType, StructType, StructField

from ..src.utils_logging import log_message

MAX_RECORDS_PARQUET_FILES = 1000000
MAX_RECORDS_CSV_FILES = 1000000
MAX_NUMBER_OF_CUSTOMERS = 100


def create_spark_context(gdp_environment, etl_date):
    """
    Create spark_session context and session
    :param gdp_environment: environment dev/qa/prod
    :param etl_date: date of etl
    :return: spark_session session and spark_session context
    """
    log_message(f"Creating Spark Session")
    spark_session = SparkSession \
        .builder \
        .appName(f"Python Spark Pipeline_{gdp_environment}_{etl_date}") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.cacheMetadata", "true") \
        .config("spark.shuffle.consolidateFiles", "true") \
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
    df_view = read_data(spark_session, file_path, csv_file)
    log_message(f"Creating {view_name} view")
    df_view.createOrReplaceTempView(view_name)


def read_data(spark_session: SparkSession, file_path: str, csv_file: bool = True) -> DataFrame:
    if not csv_file:
        log_message(f"Reading parquet {file_path}")
        read_dataframe = spark_session.read.parquet(file_path)
    else:
        log_message(f"Reading csv {file_path}")
        csv_schema = StructType([StructField("firstName", StringType(), False),
                                 StructField("lastName", StringType(), False),
                                 StructField("address", StringType(), False),
                                 StructField("dateOfBirth", StringType(), False),
                                 StructField("etl_date", StringType(), False), ])
        read_dataframe = spark_session.read.options(mode='FAILFAST', multiLine=True, escape='"') \
            .csv(path=file_path,
                 schema=csv_schema,
                 header=True)
    # log_message(f"Count of rows for {file_path}: {read_dataframe.count()}")
    return read_dataframe


def execute_sql_and_register_spark_view(spark_session, sql_stmt, view_name):
    """
    Function to execute spark sql and register as view
    :param view_name: View name to register
    :param sql_stmt: Spark SQL
    :param spark_session: Spark session
    """
    log_message(f"Reading data by executing sql")
    df_sql_exec = spark_session.sql(sql_stmt)
    log_message(f"Registering view {view_name}")
    df_sql_exec.createOrReplaceTempView(view_name)


def get_sql_stmt_df(table_name):
    """
    generates SQL for final results for SPARK execution
    :return: SQL as string
    """
    return f"""select * from {table_name}
    """


def write_results_to_disk(spark_session, table_name, output_path):
    log_message("Starting writing parquet to disk")
    df_res = spark_session.sql(get_sql_stmt_df(table_name))
    log_message(df_res.printSchema())
    df_res.repartition("etl_date").write.mode("overwrite") \
        .option("maxRecordsPerFile", MAX_RECORDS_PARQUET_FILES) \
        .parquet(output_path)
    return 0


def save_dictionary_to_csv_file(input_dictionary: list[dict], spark_session: SparkSession, output: str, etl_date: str):
    """
    Create a csv file
    :param etl_date:  date of execution of job
    :param input_dictionary: Python dictionary as input
    :param output: File location where csv should be saved
    :param spark_session: Spark Session to use for saving csv file
    :return: None
    """
    write_dataframe_to_csv(
        input_dataframe=spark_session.createDataFrame(Row(**customer) for customer in input_dictionary)
        .withColumn("etl_date", lit(etl_date))
        .repartition("etl_date"),
        output=output)


def write_dataframe_to_csv(input_dataframe: DataFrame, output):
    input_dataframe \
        .write.mode("overwrite") \
        .option('maxRecordsPerFile', MAX_RECORDS_CSV_FILES) \
        .option('header', True) \
        .csv(output)


@udf(returnType=StringType())
def generate_anonymous_first_name() -> str:
    """
    UDF to anonymise customer first name
    :return: Anonymised customer's first name
    """
    return Faker().first_name()


@udf(returnType=StringType())
def generate_anonymous_last_name() -> str:
    """
    UDF to anonymise customer last name
    :return: Anonymised customer's last name
    """
    return Faker().last_name()


@udf(returnType=StringType())
def generate_anonymous_address() -> str:
    """
    UDF to anonymise address
    :return: Anonymised customer's address
    """
    return Faker().address()


def anonymize_customer_information(df_input: DataFrame):
    customer_pii_columns = ["firstName", "lastName", "address"]
    window_spec = Window.partitionBy(customer_pii_columns).orderBy(customer_pii_columns)
    df_input = df_input.withColumn("rowNumber", row_number().over(window_spec))
    return df_input.dropDuplicates(customer_pii_columns) \
        .withColumn("anonymizedFirstName", generate_anonymous_first_name()) \
        .withColumn("anonymizedLastName", generate_anonymous_last_name()) \
        .withColumn("anonymizedAddress", generate_anonymous_address()) \
        .alias("inputDF1").join(
        df_input.alias("inputDF2"),
        [col("inputDF1.firstName") == col("inputDF2.firstName"),
         col("inputDF1.lastName") == col("inputDF2.lastName"),
         col("inputDF1.address") == col("inputDF2.address")], "inner") \
        .select(col("inputDF2.rowNumber"),
                col("inputDF1.anonymizedFirstName").alias("firstName"),
                col("inputDF1.anonymizedLastName").alias("lastName"),
                col("inputDF1.anonymizedAddress").alias("address"),
                col("inputDF2.dateOfBirth"))


def generate_dummy_customer_csv_file(path_customer_pii_table, spark_session, etl_date):
    if os.path.exists(path_customer_pii_table):
        shutil.rmtree(path_customer_pii_table)
    save_dictionary_to_csv_file(input_dictionary=generate_dummy_customer_pii_data(), spark_session=spark_session,
                                output=path_customer_pii_table, etl_date=etl_date)


def generate_dummy_customer_pii_data() -> list[dict]:
    """
    Generate dummy customer data a csv file
    :return: Dictionary of customers with their PII information
    """
    fake = Faker()
    return [{'firstName': fake.first_name(),
             'lastName': fake.last_name(),
             'address': fake.address(),
             'dateOfBirth': fake.date_of_birth()} for _ in range(MAX_NUMBER_OF_CUSTOMERS)]
