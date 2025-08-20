package com.linkedin.venice.hadoop.mapreduce.datawriter.map;

import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.KeyValueBytesPair;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;


/**
 * Mapper that reads Vson input and deserializes it as Avro object and then Avro binary
 */
public class VeniceVsonMapper extends AbstractVeniceMapper<BytesWritable, BytesWritable, KeyValueBytesPair> {
  @Override
  public AbstractVeniceRecordReader getRecordReader(VeniceProperties props) {
    return new VeniceVsonRecordReader(props);
  }

  @Override
  protected KeyValueBytesPair convertInput(BytesWritable keyBytesWritable, BytesWritable valueBytesWritable) {
    return new KeyValueBytesPair(keyBytesWritable.copyBytes(), valueBytesWritable.copyBytes());
  }
}
