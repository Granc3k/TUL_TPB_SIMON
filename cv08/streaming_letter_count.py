import argparse
import logging
import sys
import re

from pyflink.common import Types, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSource, StreamFormat


def count_words_by_letter_streaming(input_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Define sourcu + checkovani novych souboru
    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                   input_path)
                         .monitor_continuously(Duration.of_seconds(1))  # Check noveho filu kazdou 1 sekundu
                         .build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),  # Dummy watermark strategy
        source_name="file_source"
    )

    # Process kazdeho radku
    def extract_words(line):
        # Split na slova, normalizace do lowercase, a filtr podle A-Z
        words = re.findall(r'\b[a-zA-Z]+\b', line.lower())
        return [(word[0], 1) for word in words if 'a' <= word[0] <= 'z']

    # Process dat
    letter_counts = (
        ds.flat_map(extract_words, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda i: i[0])  # Group by podle first letter
          .reduce(lambda i, j: (i[0], i[1] + j[1]))  # Sum counts
    )

    # Print vysledku
    letter_counts.print()

    # Executene environment
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input directory to monitor for new files.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    count_words_by_letter_streaming(known_args.input)
