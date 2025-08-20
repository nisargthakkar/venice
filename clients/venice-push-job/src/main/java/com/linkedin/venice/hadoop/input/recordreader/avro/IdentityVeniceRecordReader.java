package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.spark.SparkConstants;
import org.apache.avro.Schema;
import org.apache.spark.sql.Row;


/**
 * A record reader that returns the input key and value as is.
 */
public class IdentityVeniceRecordReader extends AbstractVeniceRecordReader<Row> {
  private static final IdentityVeniceRecordReader INSTANCE = new IdentityVeniceRecordReader();

  private IdentityVeniceRecordReader() {
    final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
    configure(BYTES_SCHEMA, BYTES_SCHEMA);
  }

  public static IdentityVeniceRecordReader getInstance() {
    return INSTANCE;
  }

  @Override
  public Object getAvroKey(Row record) {
    throw new VeniceUnsupportedOperationException("getAvroKey in IdentityVeniceRecordReader");
  }

  @Override
  public byte[] getKeyBytes(Row record) {
    return record.getAs(SparkConstants.KEY_COLUMN_NAME);
  }

  @Override
  public Object getAvroValue(Row record) {
    throw new VeniceUnsupportedOperationException("getAvroValue in IdentityVeniceRecordReader");
  }

  @Override
  public Long getRecordTimestamp(Row record) {
    return record.getAs(SparkConstants.TIMESTAMP_COLUMN_NAME);
  }

  @Override
  public byte[] getValueBytes(Row record) {
    return record.getAs(SparkConstants.VALUE_COLUMN_NAME);
  }
}
