package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.utils.ArrayUtils;
import java.nio.ByteBuffer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IdentityVeniceRecordReaderTest {
  private static final IdentityVeniceRecordReader RECORD_READER = IdentityVeniceRecordReader.getInstance();
  private static final byte[] TEST_KEY_BYTES = "RANDOM_KEY".getBytes();
  private static final byte[] TEST_VALUE_BYTES = "RANDOM_VALUE".getBytes();

  @Test
  public void testGetKeyBytes() {
    Row record = new GenericRowWithSchema(
        new Object[] { ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES), 0L },
        SparkConstants.DEFAULT_SCHEMA);
    byte[] extractedKey = RECORD_READER.getKeyBytes(record);

    Assert.assertEquals(ArrayUtils.compareUnsigned(TEST_KEY_BYTES, extractedKey), 0);
  }

  @Test
  public void testGetValueBytes() {
    Row record = new GenericRowWithSchema(
        new Object[] { ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES), 0L },
        SparkConstants.DEFAULT_SCHEMA);
    byte[] extractedValue = RECORD_READER.getValueBytes(record);

    Assert.assertEquals(ArrayUtils.compareUnsigned(TEST_VALUE_BYTES, extractedValue), 0);
  }

  @Test
  public void testUnsupportedGetAvroData() {
    Row record = new GenericRowWithSchema(
        new Object[] { ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES), 0L },
        SparkConstants.DEFAULT_SCHEMA);
    Assert.assertThrows(VeniceUnsupportedOperationException.class, () -> RECORD_READER.getAvroKey(record));
    Assert.assertThrows(VeniceUnsupportedOperationException.class, () -> RECORD_READER.getAvroValue(record));
  }
}
