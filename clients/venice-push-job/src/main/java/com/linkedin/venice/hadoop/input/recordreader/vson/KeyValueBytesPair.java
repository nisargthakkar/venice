package com.linkedin.venice.hadoop.input.recordreader.vson;

public class KeyValueBytesPair {
  private final byte[] key;
  private final byte[] value;

  public KeyValueBytesPair(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }
}
