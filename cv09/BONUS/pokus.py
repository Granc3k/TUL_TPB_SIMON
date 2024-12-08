from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Schema, FormatDescriptor, TableDescriptor
from pyflink.table.window import Slide
from pyflink.table.expressions import col, lit

# Set up the streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# Create the Kafka source table
table_env.execute_sql("""
CREATE TABLE kafka_source (
    title STRING,
    comments INT,
    publish_date STRING,
    content STRING,
    publish_ts TIMESTAMP(3),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
)
""")

# Create sink tables
table_env.execute_sql("""
CREATE TABLE console_sink (
    title STRING
) WITH (
    'connector' = 'print'
)
""")

table_env.execute_sql("""
CREATE TABLE high_priority_sink (
    title STRING,
    comments INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/files/high_priority_articles.csv',
    'format' = 'csv'
)
""")

table_env.execute_sql("""
CREATE TABLE out_of_order_sink (
    title STRING,
    publish_date STRING,
    previous_publish_date STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/files/out_of_order_articles.csv',
    'format' = 'csv'
)
""")

table_env.execute_sql("""
CREATE TABLE window_stats_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    title STRING,
    total_articles BIGINT,
    war_mentions BIGINT
) WITH (
    'connector' = 'filesystem',
    'path' = '/files/window_stats.csv',
    'format' = 'csv'
)
""")

# Process data
articles = table_env.from_path("kafka_source")

# Print article titles to console
article_titles = articles.select(col('title'))
article_titles.execute_insert('console_sink').wait()

# Detect active articles with more than 100 comments
high_priority_articles = articles.filter(col('comments') > 100).select(col('title'), col('comments'))
high_priority_articles.execute_insert('high_priority_sink').wait()

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
out_of_order_articles.execute_insert('out_of_order_sink').wait()

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
distinct_articles.execute_insert('window_stats_sink').wait()

# Execute the pipeline
table_env.execute("article_processing_pipeline")