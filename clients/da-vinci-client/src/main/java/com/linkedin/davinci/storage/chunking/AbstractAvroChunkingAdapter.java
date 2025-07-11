package com.linkedin.davinci.storage.chunking;

import com.linkedin.davinci.listener.response.NoOpReadResponseStats;
import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;


/**
 * Read compute and write compute chunking adapter
 */
public abstract class AbstractAvroChunkingAdapter<T> implements ChunkingAdapter<ChunkedValueInputStream, T> {
  public static final int DO_NOT_USE_READER_SCHEMA_ID = -1;
  private static final int UNUSED_INPUT_BYTES_LENGTH = -1;

  @Override
  public T constructValue(
      byte[] fullBytes,
      int bytesLength,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats responseStats,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor) {
    int resolvedReaderSchemaId = resolveReaderSchemaId(readerSchemaId, writerSchemaId);
    return getByteArrayDecoder(compressor.getCompressionStrategy()).decode(
        reusedDecoder,
        fullBytes,
        bytesLength,
        reusedValue,
        storeDeserializerCache.getDeserializer(writerSchemaId, resolvedReaderSchemaId),
        responseStats,
        compressor);
  }

  @Override
  public T constructValue(
      byte[] valueOnlyBytes,
      int offset,
      int bytesLength,
      RecordDeserializer<T> recordDeserializer,
      VeniceCompressor veniceCompressor) {
    try {
      return byteArrayDecompressingDecoderValueOnly.decode(
          null,
          valueOnlyBytes,
          offset,
          bytesLength,
          null,
          veniceCompressor,
          recordDeserializer,
          NoOpReadResponseStats.SINGLETON);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addChunkIntoContainer(
      ChunkedValueInputStream chunkedValueInputStream,
      int chunkIndex,
      byte[] valueChunk) {
    chunkedValueInputStream.setChunk(chunkIndex, valueChunk);
  }

  @Override
  public ChunkedValueInputStream constructChunksContainer(ChunkedValueManifest chunkedValueManifest) {
    return new ChunkedValueInputStream(chunkedValueManifest.keysWithChunkIdSuffix.size());
  }

  @Override
  public T constructValue(
      ChunkedValueInputStream chunkedValueInputStream,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats responseStats,
      int writerSchemaId,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor) {
    int resolvedReaderSchemaId = resolveReaderSchemaId(readerSchemaId, writerSchemaId);
    return instrumentedDecompressingInputStreamDecoder.decode(
        reusedDecoder,
        chunkedValueInputStream,
        UNUSED_INPUT_BYTES_LENGTH,
        reusedValue,
        storeDeserializerCache.getDeserializer(writerSchemaId, resolvedReaderSchemaId),
        responseStats,
        compressor);
  }

  public T get(
      StorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      ReadResponseStats responseStats,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(
        this,
        store::get,
        store.getStoreVersionName(),
        partition,
        key,
        responseStats,
        reusedValue,
        reusedDecoder,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
        manifestContainer);
  }

  public ByteBufferValueRecord<T> getWithSchemaId(
      StorageEngine store,
      int partition,
      ByteBuffer key,
      boolean isChunked,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      ChunkedValueManifestContainer manifestContainer) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getValueAndSchemaIdFromStorage(
        this,
        store,
        partition,
        key,
        reusedValue,
        reusedDecoder,
        storeDeserializerCache,
        compressor,
        manifestContainer);
  }

  public T get(
      StorageEngine store,
      int partition,
      byte[] key,
      ByteBuffer reusedRawValue,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      boolean isChunked,
      ReadResponseStats responseStats,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor) {
    if (isChunked) {
      key = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key);
    }
    return ChunkingUtils.getFromStorage(
        this,
        store,
        partition,
        key,
        reusedRawValue,
        reusedValue,
        reusedDecoder,
        responseStats,
        readerSchemaId,
        storeDeserializerCache,
        compressor);
  }

  public void getByPartialKey(
      StorageEngine store,
      int userPartition,
      byte[] keyPrefixBytes,
      T reusedValue,
      BinaryDecoder reusedDecoder,
      RecordDeserializer<GenericRecord> keyRecordDeserializer,
      boolean isChunked,
      int readerSchemaId,
      StoreDeserializerCache<T> storeDeserializerCache,
      VeniceCompressor compressor,
      StreamingCallback<GenericRecord, GenericRecord> computingCallback) {

    if (isChunked) {
      throw new VeniceException("Filtering by key prefix is not supported when chunking is enabled.");
    }

    ChunkingUtils.getFromStorageByPartialKey(
        this,
        store,
        userPartition,
        keyPrefixBytes,
        reusedValue,
        keyRecordDeserializer,
        reusedDecoder,
        readerSchemaId,
        storeDeserializerCache,
        compressor,
        computingCallback);
  }

  private final DecompressingDecoderWrapperValueOnly<byte[], T> byteArrayDecompressingDecoderValueOnly = (
      reusedDecoder,
      bytes,
      offset,
      inputBytesLength,
      reusedValue,
      veniceCompressor,
      deserializer,
      readResponse) -> deserializer
          .deserialize(reusedValue, veniceCompressor.decompress(bytes, offset, inputBytesLength), reusedDecoder);

  private final DecoderWrapper<byte[], T> byteArrayDecoder =
      (reusedDecoder, bytes, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> deserializer
          .deserialize(
              reusedValue,
              ByteBuffer
                  .wrap(bytes, ValueRecord.SCHEMA_HEADER_LENGTH, inputBytesLength - ValueRecord.SCHEMA_HEADER_LENGTH),
              reusedDecoder);

  private final DecoderWrapper<InputStream, T> decompressingInputStreamDecoder =
      (reusedDecoder, inputStream, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> {
        try (InputStream decompressedInputStream = compressor.decompress(inputStream)) {
          return deserializer.deserialize(reusedValue, decompressedInputStream, reusedDecoder);
        } catch (IOException e) {
          throw new VeniceException(
              "Failed to decompress, compressionStrategy: " + compressor.getCompressionStrategy().name(),
              e);
        }
      };

  private final DecoderWrapper<byte[], T> decompressingByteArrayDecoder =
      (reusedDecoder, bytes, inputBytesLength, reusedValue, deserializer, readResponse, compressor) -> {
        try {
          // Fetch the schema id.
          int schemaId = ByteUtils.readInt(bytes, 0);
          return deserializer.deserialize(
              reusedValue,
              compressor.decompressAndPrependSchemaHeader(
                  bytes,
                  ValueRecord.SCHEMA_HEADER_LENGTH,
                  inputBytesLength - ValueRecord.SCHEMA_HEADER_LENGTH,
                  schemaId),
              reusedDecoder);
        } catch (IOException e) {
          throw new VeniceException(
              "Failed to decompress, compressionStrategy: " + compressor.getCompressionStrategy().name(),
              e);
        }
      };

  private final DecoderWrapper<byte[], T> instrumentedByteArrayDecoder =
      new InstrumentedDecoderWrapper<>(byteArrayDecoder);

  private final DecoderWrapper<byte[], T> instrumentedDecompressingByteArrayDecoder =
      new InstrumentedDecoderWrapper<>(decompressingByteArrayDecoder);

  private final DecoderWrapper<InputStream, T> instrumentedDecompressingInputStreamDecoder =
      new InstrumentedDecoderWrapper<>(decompressingInputStreamDecoder);

  private DecoderWrapper<byte[], T> getByteArrayDecoder(CompressionStrategy compressionStrategy) {
    if (compressionStrategy == CompressionStrategy.NO_OP) {
      return instrumentedByteArrayDecoder;
    } else {
      return instrumentedDecompressingByteArrayDecoder;
    }
  }

  private interface DecoderWrapper<INPUT, OUTPUT> {
    OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int inputBytesLength,
        OUTPUT reusedValue,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponseStats responseStats,
        VeniceCompressor compressor);
  }

  private interface DecompressingDecoderWrapperValueOnly<INPUT, OUTPUT> {
    OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int offset,
        int inputBytesLength,
        OUTPUT reusedValue,
        VeniceCompressor compressor,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponseStats response) throws IOException;
  }

  private static class InstrumentedDecoderWrapper<INPUT, OUTPUT> implements DecoderWrapper<INPUT, OUTPUT> {
    final private DecoderWrapper<INPUT, OUTPUT> delegate;

    InstrumentedDecoderWrapper(DecoderWrapper<INPUT, OUTPUT> delegate) {
      this.delegate = delegate;
    }

    public OUTPUT decode(
        BinaryDecoder reusedDecoder,
        INPUT input,
        int inputBytesLength,
        OUTPUT reusedValue,
        RecordDeserializer<OUTPUT> deserializer,
        ReadResponseStats responseStats,
        VeniceCompressor compressor) {
      long deserializeStartTimeInNS = responseStats.getCurrentTimeInNanos();
      OUTPUT output =
          delegate.decode(reusedDecoder, input, inputBytesLength, reusedValue, deserializer, responseStats, compressor);
      responseStats.addReadComputeDeserializationLatency(deserializeStartTimeInNS);
      return output;
    }
  }

  private int resolveReaderSchemaId(int readerSchemaId, int writerSchemaId) {
    if (readerSchemaId == DO_NOT_USE_READER_SCHEMA_ID) {
      // Venice client libraries (e.g. DVC) could pass in NON_EXISTING_SCHEMA_ID as reader schema to expect value to
      // be deserialized using the same reader schema as writer schema.
      return writerSchemaId;
    }
    return readerSchemaId;
  }
}
