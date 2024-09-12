import great_expectations as gx 
from great_expectations import expectations as gxe 


context = gx.get_context()

suite = gx.ExpectationSuite(name="my_expectation_suite")
suite = context.suites.add(suite)


#File Örneği
source_folder = "./data"
data_source = context.data_sources \
    .add_pandas_filesystem(name="my_file_data_source", 
                           base_directory=source_folder)


data_asset = data_source.add_csv_asset(name="gitbub_validation") 

batch_definition_path = "github_stats.csv" 
batch_definition = data_asset.add_batch_definition_path( 
    name="github", path=batch_definition_path 
) 

    
preset_expectation = gx.expectations \
    .ExpectColumnMaxToBeBetween(column="stars", 
                                min_value=0, 
                                max_value=10)
suite.add_expectation(preset_expectation)    


validation_definition = gx \
    .ValidationDefinition(data=batch_definition, 
                          suite=suite, 
                          name="my_validation_definition")
validation_definition = context.validation_definitions \
    .add(validation_definition)


validation_results = validation_definition.run()

print(validation_results)