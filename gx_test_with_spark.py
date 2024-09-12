import great_expectations as gx 
from great_expectations import expectations as gxe 
import findspark 
findspark.init("/data/spark-3.1.3-bin-hadoop3.2/") 
from pyspark.sql import SparkSession 
import pyspark 

#Spark Session'ı oluşturalım ve kullanacağımız dataframe'i belirleyelim
spark = SparkSession.builder.appName('gex_test').master("yarn").getOrCreate() 
parquet = "/automotive/trip_all" 
dataframe = spark.read.parquet(parquet)

context = gx.get_context()

suite = gx.ExpectationSuite(name="my_expectation_suite")
suite = context.suites.add(suite)

data_source = context.data_sources \
    .add_spark(name="my_spark_data_source")

#Postgres Örneği
#my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"
#data_source = context.data_sources \
#    .add_postgres(name="my_postgres_data_source", 
#                  connection_string=my_connection_string)

#File Örneği
#source_folder = "./data"
#data_source = context.data_sources \
#    .add_pandas_filesystem(name="my_file_data_source", 
#                           base_directory=source_folder)

#Pandas DataFrame
#data_source = context.data_sources \
#    .add_pandas(name="my_pandas_data_source")


data_asset = data_source.add_dataframe_asset(name="telemetry_validation")

batch_definition = data_asset \
    add_batch_definition_whole_dataframe("telemetry")
    
preset_expectation = gx.expectations \
    .ExpectColumnMaxToBeBetween(column="avgSpeed", 
                                min_value=0, 
                                max_value=200)
suite.add_expectation(preset_expectation)    


validation_definition = gx \
    .ValidationDefinition(data=batch_definition, 
                          suite=suite, 
                          name="my_validation_definition")
validation_definition = context.validation_definitions \
    .add(validation_definition)

batch_parameters = {"dataframe": dataframe}
validation_results = validation_definition \
    .run(batch_parameters=batch_parameters)

print(validation_results)