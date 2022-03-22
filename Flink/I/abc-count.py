import string

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors import FileSource, StreamFormat

input_path = '/files/source'

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)

#Nacteni dat
fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path).process_static_file_set().build()
ds = env.from_source(source=fs, watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),  source_name="fs")

#Mala pismena a odstraneni interpunkce
#Jen a-z
#Group By prvni pismeno
ds = ds.flat_map(lambda s: s.lower().translate(str.maketrans(dict.fromkeys(string.punctuation))).split()) \
    .filter(lambda i: i[0].isalpha())\
    .map(lambda i: (i[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda i: i[0]) \
    .reduce(lambda i, j: (i[0], i[1] + j[1])) \

ds.print()
env.execute()

