from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor

# Set up the streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# Create the Kafka source table
table_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('value', DataTypes.STRING())                
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('raw')
                .build())
        .build())

# Create the print sink table
table_env.create_temporary_table(
    'print_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('value', DataTypes.STRING())
                .build())
        .build())

# Read data from Kafka source
data = table_env.from_path("kafka_source")

# Insert data into the print sink
data.execute_insert("print_sink").wait()
