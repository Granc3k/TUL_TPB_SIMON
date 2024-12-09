from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    Schema,
    TableDescriptor,
)
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
from pyflink.table.window import Slide
from pyflink.common import Row
import json
import sys
from datetime import datetime
from dateutil import parser


def debug_print(message):
    # Print ladící zprávy na stderr (kdyby se používal grep na stdout, či jinačí hovadiny)
    print(message, file=sys.stderr)


# UDF pro parse JSONu do předdefinovaného ROW
@udf(
    result_type=DataTypes.ROW(
        [
            DataTypes.FIELD("title", DataTypes.STRING()),
            DataTypes.FIELD("comment_count", DataTypes.INT()),
            DataTypes.FIELD("publish_ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("content", DataTypes.STRING()),
        ]
    )
)
def parse_article(json_str):
    # Load JSONu, extrahuje požadovaná pole a vrátí je jako Row
    try:
        data = json.loads(json_str)
        title = data.get("title", "")
        comment_count = int(data.get("comment_count", data.get("commentCount", 0)))
        publish_date = data.get("datePublished", data.get("time", ""))

        dt = parser.isoparse(publish_date) if publish_date else None

        content = data.get("content", "")
        if isinstance(content, list):
            content = " ".join(content)

        return Row(
            title=title, comment_count=comment_count, publish_ts=dt, content=content
        )
    except Exception as e:
        debug_print(f"Failed to parse JSON: {e}")
        return Row(title="", comment_count=0, publish_ts=None, content="")


# Settings enviromentu pro stream processing
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

debug_print("Environment set up.")
debug_print("Creating tables...")

# Define source tabulky z Kafky
table_env.create_temporary_table(
    "kafka_source",
    TableDescriptor.for_connector("kafka")
    .schema(
        Schema.new_builder()
        .column("value", DataTypes.STRING())
        .column_by_expression("process_time", "PROCTIME()")
        .build()
    )
    .option("topic", "test-topic")
    .option("properties.bootstrap.servers", "kafka:9092")
    .option("properties.group.id", "flink-group")
    .option("scan.startup.mode", "latest-offset")
    .format("raw")
    .build(),
)

# Output do konzole
table_env.create_temporary_table(
    "console_sink",
    TableDescriptor.for_connector("print")
    .schema(Schema.new_builder().column("title", DataTypes.STRING()).build())
    .build(),
)

# Output articlů s > 100 komentáři
table_env.create_temporary_table(
    "high_priority_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(
        Schema.new_builder()
        .column("title", DataTypes.STRING())
        .column("comment_count", DataTypes.INT())
        .build()
    )
    .option("path", "/files/out/high_priority_articles")
    .format("csv")
    .build(),
)

# Output out-of-order articlů
table_env.create_temporary_table(
    "out_of_order_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(
        Schema.new_builder()
        .column("title", DataTypes.STRING())
        .column("publish_date", DataTypes.STRING())
        .column("prev_publish_date", DataTypes.STRING())
        .build()
    )
    .option("path", "/files/out/out_of_order_articles")
    .format("csv")
    .build(),
)

# Output statistik do filu
table_env.create_temporary_table(
    "counts_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(
        Schema.new_builder()
        .column("window_start", DataTypes.TIMESTAMP(3))
        .column("window_end", DataTypes.TIMESTAMP(3))
        .column("total_count", DataTypes.BIGINT())
        .column("war_articles", DataTypes.BIGINT())
        .build()
    )
    .option("path", "/files/out/counts")
    .format("csv")
    .option("sink.rolling-policy.rollover-interval", "1 min")
    .option("sink.rolling-policy.file-size", "1MB")
    .build(),
)

debug_print("Tables created.")
debug_print("Creating pipeline...")

# Load a parse dat z Kafky
parsed_data = (
    table_env.from_path("kafka_source")
    .select(parse_article(col("value")).alias("r"), col("process_time"))
    .select(
        col("r").get("title").alias("title"),
        col("r").get("comment_count").alias("comment_count"),
        col("r").get("publish_ts").alias("publish_ts"),
        col("r").get("content").alias("content"),
        col("process_time"),
    )
)

# Create dočasného pohledu
table_env.create_temporary_view("parsed_data_view", parsed_data)

# SQL dotaz pro detekci out-of-order
# použití LAG() fce z SQL k získání předchozího publish_ts
# následné porovnání současného a předchozího publish_ts
out_of_order = table_env.sql_query(
    """
SELECT 
    title,
    CAST(publish_ts AS STRING) AS publish_date,
    CAST(prev_publish_ts AS STRING) AS prev_publish_date
FROM (
    SELECT
        title,
        publish_ts,
        LAG(publish_ts) OVER (ORDER BY process_time) AS prev_publish_ts
    FROM parsed_data_view
) t
WHERE prev_publish_ts IS NOT NULL AND publish_ts < prev_publish_ts
"""
)

# Agregace countů a válka-articlů v time oknech
counts_table = (
    table_env.from_path("parsed_data_view")
    .window(
        Slide.over(lit(1).minutes)
        .every(lit(10).seconds)
        .on(col("process_time"))
        .alias("w")
    )
    .group_by(col("w"))
    .select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit(1).count.alias("total_count"),
        col("content").like("%válka%").cast(DataTypes.INT()).sum.alias("war_articles"),
    )
)

pipeline = table_env.create_statement_set()

# Printy do jednotlivých sinků
pipeline.add_insert(
    "console_sink", table_env.from_path("parsed_data_view").select(col("title"))
)
pipeline.add_insert(
    "high_priority_sink",
    table_env.from_path("parsed_data_view")
    .filter(col("comment_count") > 100)
    .select(col("title"), col("comment_count")),
)
pipeline.add_insert("out_of_order_sink", out_of_order)
pipeline.add_insert("counts_sink", counts_table)

debug_print("Pipeline created.")
debug_print("Executing pipeline...")

pipeline.execute().wait()

debug_print("Pipeline execution completed.")
