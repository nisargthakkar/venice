package com.linkedin.venice.hadoop.mapreduce.datawriter.map;

import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;


public class VeniceAvroMapper extends AbstractVeniceMapper<AvroWrapper<IndexedRecord>, NullWritable, IndexedRecord> {
  @Override
  public VeniceAvroRecordReader getRecordReader(VeniceProperties props) {
    return VeniceAvroRecordReader.fromProps(props);
  }

  @Override
  protected IndexedRecord convertInput(AvroWrapper<IndexedRecord> indexedRecordAvroWrapper, NullWritable nullWritable) {
    return indexedRecordAvroWrapper.datum();
  }
}
