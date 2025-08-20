package com.linkedin.venice.hadoop.input.recordreader;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;


/**
 * An abstraction for a record reader that reads records from the configured input into Avro-serialized keys and values.
 * @param <T> The format of the input as controlled by the input format
 */
public abstract class AbstractVeniceRecordReader<T> {
  private Schema keySchema;
  private Schema valueSchema;

  private RecordSerializer<Object> keySerializer;
  private RecordSerializer<Object> valueSerializer;

  public Schema getKeySchema() {
    return keySchema;
  }

  public Schema getValueSchema() {
    return valueSchema;
  }

  /**
   * Configure the record serializers
   */
  protected void configure(Schema keySchema, Schema valueSchema) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
  }

  /**
   * Return an Avro output key
   */
  public abstract Object getAvroKey(T inputObj);

  /**
   * return an Avro output value
   */
  public abstract Object getAvroValue(T inputObj);

  public abstract Long getRecordTimestamp(T inputObj);

  /**
   * Return a serialized output key
   */
  public byte[] getKeyBytes(T inputObj) {
    if (keySerializer == null) {
      throw new VeniceException("Record reader must be configured before calling getKeyBytes");
    }

    Object avroKey = getAvroKey(inputObj);

    if (avroKey == null) {
      return null;
    }

    return keySerializer.serialize(avroKey);
  }

  /**
   * Return a serialized output value
   */
  public byte[] getValueBytes(T inputObj) {
    if (valueSerializer == null) {
      throw new VeniceException("Record reader must be configured before calling getValueBytes");
    }

    Object avroValue = getAvroValue(inputObj);

    if (avroValue == null) {
      return null;
    }

    return valueSerializer.serialize(avroValue);
  }
}
