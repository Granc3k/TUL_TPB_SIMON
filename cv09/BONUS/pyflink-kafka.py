from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.table.window import Slide
from pyflink.table.expressions import col, lit

# Set up the streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# Create the Kafka source table
table_env.create_temporary_table(
    'kafka_source',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .column('publish_date', DataTypes.STRING())
                .column('content', DataTypes.STRING())
                .column('publish_ts', DataTypes.TIMESTAMP(3))
                .column('event_time', DataTypes.TIMESTAMP(3))
                .watermark('event_time', 'event_time - INTERVAL \'5\' SECOND')
                .build())
        .option('topic', 'test-topic')
        .option('properties.bootstrap.servers', 'kafka:9092')
        .option('properties.group.id', 'flink-group')
        .option('scan.startup.mode', 'latest-offset')
        .format(FormatDescriptor.for_format('json').build())
        .build())

# Create sink tables
table_env.create_temporary_table(
    'console_sink',
    TableDescriptor.for_connector("print")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .build())
        .build())

table_env.create_temporary_table(
    'high_priority_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('comments', DataTypes.INT())
                .build())
        .option("path", "/files/out/high_priority_articles")
        .format("csv")
        .build())

table_env.create_temporary_table(
    'out_of_order_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('title', DataTypes.STRING())
                .column('publish_date', DataTypes.STRING())
                .column('previous_publish_date', DataTypes.STRING())
                .build())
        .option("path", "/files/out/out_of_order_articles")
        .format("csv")
        .build())

table_env.create_temporary_table(
    'window_stats_sink',
    TableDescriptor.for_connector("filesystem")
        .schema(Schema.new_builder()
                .column('window_start', DataTypes.TIMESTAMP(3))
                .column('window_end', DataTypes.TIMESTAMP(3))
                .column('title', DataTypes.STRING())
                .column('total_articles', DataTypes.BIGINT())
                .column('war_mentions', DataTypes.BIGINT())
                .build())
        .option("path", "/files/out/window_stats")
        .format("csv")
        .build())

# Process data
articles = table_env.from_path("kafka_source")

# Print article titles to console
article_titles = articles.select(col('title'))

# Detect active articles with more than 100 comments
high_priority_articles = articles.filter(col('comments') > 100).select(col('title'), col('comments'))

# Detect out-of-order articles
articles = articles.add_or_replace_columns(
    col("publish_date").cast(DataTypes.TIMESTAMP(3)).alias("publish_ts")
)
out_of_order_articles = articles.filter(
    col("publish_ts") < lit("1970-01-01 00:00:00.000").cast(DataTypes.TIMESTAMP(3))  # Example condition
).select(
    col('title'),
    col('publish_date'),
    lit("1970-01-01 00:00:00.000").alias("previous_publish_date")
)

# Create windowed table
windowed_articles = articles\
    .window(Slide
        .over(lit(1).minutes)
        .every(lit(10).seconds)
        .on(col('event_time'))
        .alias('w')
    )\
    .group_by(col('w'), col('title'))\
    .select(
        col('w').start.alias('window_start'),
        col('w').end.alias('window_end'),
        col('title'),
        col('title').count.alias('total_articles'),
        col('content').like('%válka%').count.alias('war_mentions')  
    )

# Check for duplicates based on title
distinct_articles = windowed_articles.distinct()

# Create a statement set for executing multiple inserts
pipeline = table_env.create_statement_set()
pipeline.add_insert('console_sink', article_titles)
pipeline.add_insert('high_priority_sink', high_priority_articles)
pipeline.add_insert('out_of_order_sink', out_of_order_articles)
pipeline.add_insert('window_stats_sink', distinct_articles)

# Execute the pipeline
pipeline.execute().wait()

# funguje čtení kafky ale neukládá do souborů