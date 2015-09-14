package com.cloudera.science.avro.streaming;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.mapred.Container;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;

import java.io.IOException;

public class ParquetAsJSONInputFormat extends AvroAsJSONInputFormat {

    private DeprecatedParquetInputFormat<IndexedRecord> realInputFormat = new DeprecatedParquetInputFormat<IndexedRecord>();

    /**
     * key to configure the ReadSupport implementation
     */
    public static final String READ_SUPPORT_CLASS = "parquet.read.support.class";
    public static final Log LOG =
            LogFactory.getLog(ParquetAsJSONInputFormat.class);

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

        LOG.debug("Entering getRecordReader");
        RecordReader<Void, Container<IndexedRecord>> realRecordReader = realInputFormat.getRecordReader(split, job, reporter);

        return new ParquetAsJSONRecordReader(realRecordReader, split);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        job.set(READ_SUPPORT_CLASS, AvroReadSupport.class.getName());
        return realInputFormat.getSplits(job, numSplits);
    }



    private static class ParquetAsJSONRecordReader implements RecordReader<Text, Text> {

        RecordReader<Void, Container<IndexedRecord>> realRecordReader;
        private Container<IndexedRecord> realValue = null;
        private GenericRecord datum = null;
        private Void realKey = null;

        private long splitLen; // for getPos()

        public ParquetAsJSONRecordReader(RecordReader<Void, Container<IndexedRecord>> recordReader, InputSplit split)
            throws IOException {
            realRecordReader = recordReader;
            realValue = recordReader.createValue();
            splitLen = split.getLength();
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (realRecordReader.next(realKey, realValue)) {
                datum = (GenericRecord) realValue.get();
                key.set(datum.toString());
                return true;
            }
            return false;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return (long) (splitLen * getProgress());
        }

        @Override
        public void close() throws IOException {
            realRecordReader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return realRecordReader.getProgress();
        }
    }
}

