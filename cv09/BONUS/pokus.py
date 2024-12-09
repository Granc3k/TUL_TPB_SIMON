from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    Schema,
    TableDescriptor,
)
from pyflink.table.expressions import col
from pyflink.table import expressions as expr
import sys


# Fce pro print debug infa do stderr
def debug_print(message):
    print(message, file=sys.stderr)


# Set upnutí stream env
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

debug_print("Environment set up.")
debug_print("Creating tables...")

# Create source tejbl pro Kafku
table_env.create_temporary_table(
    "kafka_source",
    TableDescriptor.for_connector("kafka")
    .schema(Schema.new_builder().column("value", DataTypes.STRING()).build())
    .option("topic", "test-topic")
    .option("properties.bootstrap.servers", "kafka:9092")
    .option("properties.group.id", "flink-group")
    .option("scan.startup.mode", "latest-offset")
    .format("raw")
    .build(),
)

# Creatnutí sink tejblů
# Sink na raw data do konzole
table_env.create_temporary_table(
    "console_sink",
    TableDescriptor.for_connector("print")
    .schema(Schema.new_builder().column("data", DataTypes.STRING()).build())
    .option("print-identifier", "")
    .build(),
)

# High priority artikly tejbla
table_env.create_temporary_table(
    "high_priority_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder().column("article_info", DataTypes.STRING()).build())
    .option("path", "/files/out/high_priority_articles")
    .format("csv")
    .build(),
)

# Out of order artikly tejbla
table_env.create_temporary_table(
    "out_of_order_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder().column("article_info", DataTypes.STRING()).build())
    .option("path", "/files/out/out_of_order_articles")
    .format("csv")
    .build(),
)

# Basic count sink tejbla
table_env.create_temporary_table(
    "counts_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(Schema.new_builder().column("stats", DataTypes.STRING()).build())
    .option("path", "/files/out/counts")
    .format("csv")
    .build(),
)

debug_print("Tables created.")
debug_print("Creating pipeline...")

# Create pipeline
pipeline = table_env.create_statement_set()

# Base tejbla
kafka_table = table_env.from_path("kafka_source")

# Raw data do konzole
pipeline.add_insert("console_sink", kafka_table.select(col("value").alias("data")))

# Priority artikly
pipeline.add_insert(
    "high_priority_sink", kafka_table.select(col("value").alias("article_info"))
)

# Out of order artikly
pipeline.add_insert(
    "out_of_order_sink", kafka_table.select(col("value").alias("article_info"))
)

# Basic county
pipeline.add_insert("counts_sink", kafka_table.select(col("value").alias("stats")))

debug_print("Pipeline created.")
debug_print("Executing pipeline...")

# Executnutí pajplajny
pipeline.execute().wait()

debug_print("Pipeline execution completed.")
