To Build:

	mvn clean package

Hive Usage:

	add jar avro-json-1.0-SNAPSHOT.jar;
	CREATE TABLE doctors (foo string)
	ROW FORMAT SERDE 'com.cloudera.science.avro.serde.AvroAsJSONSerde'
	INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	WITH SERDEPROPERTIES
	('avro.schema.literal'='{
	  "namespace": "testing.hive.avro.serde",
	  "name": "doctors",
	  "type": "record",
	  "fields": [
	    {
	      "name":"number",
	      "type":"int",
	      "doc":"Order of playing the role"
	    },
	    {
	      "name":"first_name",
	      "type":"string",
	      "doc":"first name of actor playing role"
	    },
	    {
	      "name":"last_name",
	      "type":"string",
	      "doc":"last name of actor playing role"
	    },
	    {
	      "name":"extra_field",
	      "type":"string",
	      "doc:":"an extra field not in the original file",
	      "default":"fishfingers and custard"
	    }
	  ]
	}');
	DESCRIBE doctors;
	SELECT * from doctors;
	SELECT get_json_object(foo, '$.number') from doctors;

Streaming Usage:

	hadoop jar hadoop-streaming-2.0.0-mr1-cdh4.1.2.jar \
	-libjars avro-json-1.0-SNAPSHOT.jar \
	-Dinput.schema.url=file:///doctors.avsc \
	-Doutput.schema.url=file:///doctors.avsc \
	-inputformat com.cloudera.science.avro.streaming.AvroAsJSONInputFormat \
	-outputformat com.cloudera.science.avro.streaming.AvroAsJSONOutputFormat \
	-mapper '/bin/cat' \
	-input doctors.avro \
	-output foo

Streaming Over Multiple Files with Different Schemas (Make sure that the ordering of the
arguments to input.schema.url corresponds to the ordering of the -input args to streaming):

	hadoop jar hadoop-streaming-2.0.0-mr1-cdh4.1.2.jar \
	-libjars avro-json-1.0-SNAPSHOT.jar \
	-Dinput.schema.url=file:///schemas/doctors.avsc,file:///schemas/episodes.avsc \
	-inputformat com.cloudera.science.avro.streaming.AvroAsJSONInputFormat \
	-mapper '/bin/cat' \
	-input /data/doctors.avro \
	-input /data/episodes.avro \
	-output multijson

