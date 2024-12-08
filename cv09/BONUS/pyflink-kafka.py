from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    DataTypes,
    Schema,
    FormatDescriptor,
    TableDescriptor,
)
from pyflink.table.udf import udf
from pyflink.table.expressions import col, lit
from datetime import datetime
import json

# Funkce pro převod JSON stringu na dictionary
def parse_json(value):
    try:
        return json.loads(value)
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        return {}

# UDFs pro parsing atributů z JSON
@udf(result_type=DataTypes.STRING())
def parse_title(value):
    return parse_json(value).get("title", "")

@udf(result_type=DataTypes.INT())
def parse_comment_count(value):
    return parse_json(value).get("commentCount", 0)

@udf(result_type=DataTypes.STRING())
def parse_content(value):
    article = parse_json(value)
    content_items = article.get("content", [])
    if isinstance(content_items, list):
        # Spojíme všechny části obsahu do jednoho řetězce
        return " ".join(content_items).replace("\n", " ")
    return ""


@udf(result_type=DataTypes.STRING())
def parse_date_published(value):
    return parse_json(value).get("datePublished", "")

@udf(result_type=DataTypes.STRING())
def current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Nastavení prostředí
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(environment_settings=env_settings)

# Kafka zdroj
table_env.create_temporary_table(
    "kafka_source",
    TableDescriptor.for_connector("kafka")
    .schema(Schema.new_builder().column("value", DataTypes.STRING()).build())
    .option("topic", "test-topic")
    .option("properties.bootstrap.servers", "kafka:9092")
    .option("properties.group.id", "flink-group")
    .option("scan.startup.mode", "latest-offset")
    .format(FormatDescriptor.for_format("raw").build())
    .build()
)

# Sink pro tabulku článků
table_env.create_temporary_table(
    "article_table_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(
        Schema.new_builder()
        .column("title", DataTypes.STRING())
        .column("comment_count", DataTypes.INT())
        .column("content", DataTypes.STRING())
        .column("date_published", DataTypes.STRING())
        .column("created_at", DataTypes.STRING())
        .build()
    )
    .option("path", "/files/out/article_table")
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", ";").build())
    .build()
)

# Sink pro názvy článků do konzole
table_env.create_temporary_table(
    "article_title_log_sink",
    TableDescriptor.for_connector("print")
    .schema(Schema.new_builder().column("title", DataTypes.STRING()).build())
    .build()
)

# Sink pro aktivní články
table_env.create_temporary_table(
    "active_articles_sink",
    TableDescriptor.for_connector("filesystem")
    .schema(
        Schema.new_builder()
        .column("title", DataTypes.STRING())
        .column("comment_count", DataTypes.INT())
        .build()
    )
    .option("path", "/files/out/active_articles")
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", ";").build())
    .build()
)

# Zpracování dat
data = table_env.from_path("kafka_source")

# Zpracovaná tabulka článků
article_table = data.select(
    parse_title(data.value).alias("title"),
    parse_comment_count(data.value).alias("comment_count"),
    parse_content(data.value).alias("content"),
    parse_date_published(data.value).alias("date_published"),
    current_time().alias("created_at"),
)

# Filtrování článků s více než 100 komentáři
active_articles = article_table.filter(col("comment_count") > 100).select(
    col("title"), col("comment_count")
)

# Dotaz pro vypisování názvu zpracovávaného článku
article_title_log = article_table.select(article_table.title)

# Dotaz pro ukládání názvu článku, který má více jak 100 komentářů
active_articles = article_table.select(article_table.title, article_table.comment_count).filter(
    article_table.comment_count > 100
)

# Přidání debug logů
print("Pipeline is being built...")

# Zápis výsledků do sinků pomocí pipeliny
pipeline = table_env.create_statement_set()
pipeline.add_insert("article_table_sink", article_table)
pipeline.add_insert("article_title_log_sink", article_title_log)
pipeline.add_insert("active_articles_sink", active_articles)

# Kontrola pipeline
print("Pipeline prepared, executing now...")
pipeline_result = pipeline.execute()
pipeline_result.wait()
print("Pipeline execution finished.")

