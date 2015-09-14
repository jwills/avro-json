package com.cloudera.science.avro.streaming;

import java.io.File;

import org.apache.avro.RandomData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileUtil;

import org.junit.Test;
import org.junit.Assert;
import parquet.avro.AvroParquetWriter;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;

public class TestParquetAsJSONRecordReader {

    private static Path outDir = new Path(System.getProperty("test.build.data",
            "/tmp"), "TestParquetAsJSONRecordReader");

    private static String[] schemaStrings = new String[] {
            "{\"type\": \"record\", \"name\": \"org.foo.Foo\",\n"+
            " \"fields\": [ {\"name\": \"x\", \"type\": \"int\"}, {\"name\": \"y\", \"type\": \"string\"} ]\n"+
            "}",
            "{\"type\": \"record\", \"name\": \"org.foo.Foo\",\n"+
            " \"fields\": [ {\"name\": \"x\", \"type\": \"int\"}, {\"name\": \"y\", \"type\": \"string\"}, "+
                "{\"name\": \"z\", \"type\": \"string\"}]}",
            "{\"type\": \"record\", \"name\": \"org.foo.Foo\",\n"+
                    " \"fields\": [ {\"name\": \"x\", \"type\": \"int\"}, {\"name\": \"y\", \"type\": \"string\"} ]\n"+
                    "}",
    };

    private static Schema[] schemas = {
            new Schema.Parser().parse(schemaStrings[0]),
            new Schema.Parser().parse(schemaStrings[1]),
            new Schema.Parser().parse(schemaStrings[2])
    };

    private static final Log LOG = LogFactory.getLog(ParquetAsJSONInputFormat.class);

    private static void createRandomData(Path p, Schema schema) {

        try {
            AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(p, schema);
            for (Object datum : new RandomData(schema, 5))
                writer.write((GenericRecord) datum);

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRecordReader() throws IOException{

        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", "file:///");
        FileSystem localFs = FileSystem.getLocal(conf);

        Path[] paths = new Path[3];
        long[] fileLength = new long[3];
        File[] files = new File[3];
        HashMap<String, Schema> pathSchemas = new HashMap<String, Schema>();
        //Text key = new Text();
        //Text value = new Text();
        ParquetAsJSONInputFormat inputFormat = new ParquetAsJSONInputFormat();

        try {
            //Create three test Parquet files
            for(int i=0;i<3;i++) {
                File dir = new File(outDir.toString());
                dir.mkdir();
                files[i] = new File(dir,"testfile"+i+".parquet");
                paths[i] = new Path(outDir + "/testfile"+i+".parquet").makeQualified(localFs);
                createRandomData(paths[i], schemas[i]);
                fileLength[i] = files[i].length();
                pathSchemas.put(paths[i].toString(), schemas[i]);
            }

            Reporter reporter = Reporter.NULL;
            FileInputFormat.setInputPaths(conf, outDir);
            InputSplit[] splits = inputFormat.getSplits(conf, 2);

            int count = 0;
            for (int i=0;i<3; i++) {
                int subcount = 0;
                RecordReader<Text, Text> reader = inputFormat.getRecordReader(splits[i], conf, reporter);
                Text key = reader.createKey();
                Class keyClass = key.getClass();
                Text value = reader.createValue();
                Class valueClass = value.getClass();
                Assert.assertEquals("Key class is Text.", Text.class, keyClass);
                Assert.assertEquals("Value class is Text.", Text.class, valueClass);


                while (reader.next(key, value)) {
                    count += 1;
                    subcount += 1;
                    System.err.println("key: " + key.toString() + "\t" + "value: " + value.toString() + "\n");

                }
                Assert.assertEquals(5, subcount);
            }

            Assert.assertEquals(15, count);
        } finally {
            FileUtil.fullyDelete(new File(outDir.toString()));
        }

    }
}
