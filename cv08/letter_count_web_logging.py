# made by Martin "Granc3k" Šimon
import argparse
import logging
import sys
import re

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FileSource, StreamFormat


def count_words_by_letter(input_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)  # Setnutní paralelismu na 1

    # Define sourců
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), input_path
        )
        .process_static_file_set()
        .build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source",
    )

    # Process řádků
    def extract_words(line):
        # Split na slova, normalizace na lowercase, a filtr podle A-Z
        words = re.findall(r"\b[a-zA-Z]+\b", line.lower())
        return [(word[0], 1) for word in words if "a" <= word[0] <= "z"]

    # Process dat
    letter_counts = (
        ds.flat_map(
            extract_words, output_type=Types.TUPLE([Types.STRING(), Types.INT()])
        )
        .key_by(lambda i: i[0])  # Group by podle 1. písmene
        .reduce(lambda i, j: (i[0], i[1] + j[1]))  # Sum počtů
    )

    # Loging výsledků
    def log_results(letter_count):
        letter, count = letter_count
        logging.info(f"{letter}: {count}")

    # Přidání logování výsledků
    letter_counts.map(lambda x: log_results(x))

    # Start prostředí
    env.execute("Count Words by Letter")


if __name__ == "__main__":
    # Conf logování
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        required=True,
        help="Input directory containing text files to process.",
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    count_words_by_letter(known_args.input)
