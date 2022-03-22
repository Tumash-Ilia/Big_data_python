import string

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common.time import Duration

input_path = '/files/stream'

env = StreamExecutionEnvironment.get_execution_environment()

fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path)\
    .monitor_continuously(Duration.of_seconds(5)).build()

ds = env.from_source(source=fs, watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),  source_name="fs")

ds = ds.flat_map(lambda s: s.lower().translate(str.maketrans(dict.fromkeys(string.punctuation))).split()) \
    .filter(lambda i: i[0].isalpha())\
    .map(lambda i: (i[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda i: i[0]) \
    .reduce(lambda i, j: (i[0], i[1] + j[1])) \

ds.print()
env.execute()

