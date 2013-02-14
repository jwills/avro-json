To Build:

	mvn clean package

Usage:

	add jar avro-json-serde-1.0-SNAPSHOT.jar;
	FROM DOCTORS
	SELECT TRANSFORM(*)
	ROW FORMAT SERDE 'com.cloudera.science.avro.serde.AvroJSONSerde'
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
	}')
	USING '/bin/cat';

