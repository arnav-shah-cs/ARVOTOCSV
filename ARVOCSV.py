from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
import csv
import os

# Make sure to define the location of the Avro file and output folder for program to work.
# On macOS, this looks like:
# avro_file_path = "/Users/Arnav/Data/Avros/1-1-100_1707311064.avro"
# output_dir = "/Users/Arnav/Data/Output/"
#
# On Windows, this looks like:
# avro_file_path = "C:/Data/Avros/1-1-100_1707311064.avro"
# output_dir = "C:/Data/Output/"
avro_file_path = "[insert the avro filename with the full path here to upload data]"
output_dir = "[insert the path to a directory where to save csv files for each ARVO file]"

# Read Avro file
reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
data= next(reader)

# Uncomment the below 2 lines to print the Avro schema
# print(schema)
# print(" ")

# Eda - need this, skin conductance
eda = data["rawData"]["eda"]
timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
    for i in range(len(eda["values"]))]
with open(os.path.join(output_dir, 'eda.csv'), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["unix_timestamp", "eda"])
    writer.writerows([[ts, eda] for ts, eda in zip(timestamp, eda["values"])])

# Temperature - need this
tmp = data["rawData"]["temperature"]
timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
    for i in range(len(tmp["values"]))]
with open(os.path.join(output_dir, 'temperature.csv'), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["unix_timestamp", "temperature"])
    writer.writerows([[ts, tmp] for ts, tmp in zip(timestamp, tmp["values"])])

# Tags - keeping in, not sure if relevant
tags = data["rawData"]["tags"]
with open(os.path.join(output_dir, 'tags.csv'), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["tags_timestamp"])
    writer.writerows([[tag] for tag in tags["tagsTimeMicros"]])

# Heart Rate - need this section
# dependent on heart rate being stored under header in ARVO: data["rawData"]["heartRate"]
hr = data["rawData"]["heartRate"]
timestamp = [round(hr["timestampStart"] + i * (1e6 / hr["samplingFrequency"]))
    for i in range(len(hr["values"]))]
with open(os.path.join(output_dir, 'heart_rate.csv'), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["unix_timestamp", "heart_rate"])
    writer.writerows([[ts, hr] for ts, hr in zip(timestamp, hr["values"])])

