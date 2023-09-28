import avro.schema
import io
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder
from avro.datafile import DataFileReader, DataFileWriter

# SCHEMA
schema = avro.schema.parse(open("user.avsc", "rb").read())

# WRITER
writer = DatumWriter(schema)

bytes_writer = io.BytesIO()
print(writer)

encoder = BinaryEncoder(bytes_writer)
print(encoder)

writer.write({"name": "Alyssa", "favorite_number": [256, 59.2]}, encoder)

raw_bytes = bytes_writer.getvalue()
print(len(raw_bytes))
print(type(raw_bytes))

# READER
bytes_reader = io.BytesIO(raw_bytes)
decoder = BinaryDecoder(bytes_reader)
reader = DatumReader(schema)
user1 = reader.read(decoder)

print(user1)

# writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
# writer.append({"name": "Alyssa", "favorite_number": [256, 59]})
# writer.append({"name": "Ben", "favorite_number": [7, 1.09], "favorite_color": "red"})
# writer.close()

# reader = DataFileReader(open("users.avro", "rb"), DatumReader())

# print(reader)
# for user in reader:
#     print(user)

# reader.close()
