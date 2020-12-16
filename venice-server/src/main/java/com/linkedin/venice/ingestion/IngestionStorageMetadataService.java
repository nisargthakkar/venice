package com.linkedin.venice.ingestion;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.meta.IngestionMetadataUpdateType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageMetadataService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ingestion.IngestionUtils.*;


/**
 * IngestionStorageMetadataService is an in-memory storage metadata service for Da Vinci backend with ingestion isolation.
 * It keeps storage metadata in the memory so RocksDB metadata partitions can be opened by isolated ingestion process only.
 * For metadata update generated by hybrid ingestion, it will sync and persist the update to the RocksDB metadata partition
 * through IPC protocol.
 */
public class IngestionStorageMetadataService extends AbstractVeniceService implements StorageMetadataService {
  private static final Logger logger = Logger.getLogger(IngestionStorageMetadataService.class);

  private final IngestionRequestClient client;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;
  private final Map<String, Map<Integer, OffsetRecord>> topicPartitionOffsetRecordMap = new VeniceConcurrentHashMap<>();
  private final Map<String, StoreVersionState> topicStoreVersionStateMap = new VeniceConcurrentHashMap<>();

  public IngestionStorageMetadataService(int targetPort, InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.client = new IngestionRequestClient(targetPort);
    this.partitionStateSerializer = partitionStateSerializer;
  }

  @Override
  public boolean startInner() throws Exception {
    return false;
  }

  @Override
  public void stopInner() throws Exception {
    client.close();
  }

  @Override
  public void put(String topicName, StoreVersionState record) throws VeniceException {
    putStoreVersionState(topicName, record);
    // Sync update with metadata partition opened by ingestion process.
    IngestionStorageMetadata ingestionStorageMetadata = new IngestionStorageMetadata();
    ingestionStorageMetadata.metadataUpdateType = IngestionMetadataUpdateType.PUT_STORE_VERSION_STATE.getValue();
    ingestionStorageMetadata.topicName = topicName;
    ingestionStorageMetadata.payload = ByteBuffer.wrap(IngestionUtils.serializeStoreVersionState(topicName, record));
    updateRemoteStorageMetadataService(ingestionStorageMetadata);
  }

  @Override
  public void clearStoreVersionState(String topicName) {
    logger.info("Clearing StoreVersionState for " + topicName);
    topicStoreVersionStateMap.remove(topicName);
    // Sync update with metadata partition opened by ingestion process.
    IngestionStorageMetadata ingestionStorageMetadata = new IngestionStorageMetadata();
    ingestionStorageMetadata.metadataUpdateType = IngestionMetadataUpdateType.CLEAR_STORE_VERSION_STATE.getValue();
    ingestionStorageMetadata.topicName = topicName;
    ingestionStorageMetadata.payload = ByteBuffer.wrap(new byte[0]);
    updateRemoteStorageMetadataService(ingestionStorageMetadata);
  }

  @Override
  public Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException {
    if (topicStoreVersionStateMap.containsKey(topicName)) {
      return Optional.of(topicStoreVersionStateMap.get(topicName));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    putOffsetRecord(topicName, partitionId, record);
    // Sync update with metadata partition opened by ingestion process.
    IngestionStorageMetadata ingestionStorageMetadata = new IngestionStorageMetadata();
    ingestionStorageMetadata.metadataUpdateType = IngestionMetadataUpdateType.PUT_OFFSET_RECORD.getValue();
    ingestionStorageMetadata.topicName = topicName;
    ingestionStorageMetadata.partitionId = partitionId;
    ingestionStorageMetadata.payload = ByteBuffer.wrap(record.toBytes());
    updateRemoteStorageMetadataService(ingestionStorageMetadata);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    logger.info("Clearing OffsetRecord for " + topicName + " " + partitionId);
    if (topicPartitionOffsetRecordMap.containsKey(topicName)) {
      Map<Integer, OffsetRecord> partitionOffsetRecordMap = topicPartitionOffsetRecordMap.get(topicName);
      partitionOffsetRecordMap.remove(partitionId);
      topicPartitionOffsetRecordMap.put(topicName, partitionOffsetRecordMap);
    }
    // Sync update with metadata partition opened by ingestion process.
    IngestionStorageMetadata ingestionStorageMetadata = new IngestionStorageMetadata();
    ingestionStorageMetadata.metadataUpdateType = IngestionMetadataUpdateType.CLEAR_OFFSET_RECORD.getValue();
    ingestionStorageMetadata.topicName = topicName;
    ingestionStorageMetadata.partitionId = partitionId;
    ingestionStorageMetadata.payload = ByteBuffer.wrap(new byte[0]);
    updateRemoteStorageMetadataService(ingestionStorageMetadata);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    if (topicPartitionOffsetRecordMap.containsKey(topicName)) {
      Map<Integer, OffsetRecord> partitionOffsetRecordMap = topicPartitionOffsetRecordMap.get(topicName);
      return partitionOffsetRecordMap.getOrDefault(partitionId, new OffsetRecord(partitionStateSerializer));
    }
    return new OffsetRecord(partitionStateSerializer);
  }

  /**
   * putOffsetRecord will only put OffsetRecord into in-memory state, without persisting into metadata RocksDB partition.
   */
  public void putOffsetRecord(String topicName, int partitionId, OffsetRecord record) {
    logger.info("Updating OffsetRecord for " + topicName + " " + partitionId + " " + record.getOffset());
    Map<Integer, OffsetRecord> partitionOffsetRecordMap = topicPartitionOffsetRecordMap.getOrDefault(topicName, new VeniceConcurrentHashMap<>());
    partitionOffsetRecordMap.put(partitionId, record);
    topicPartitionOffsetRecordMap.put(topicName, partitionOffsetRecordMap);
  }

  /**
   * putStoreVersionState will only put StoreVersionState into in-memory state, without persisting into metadata RocksDB partition.
   */
  public void putStoreVersionState(String topicName, StoreVersionState record) {
    if (logger.isDebugEnabled()) {
      logger.debug("Updating StoreVersionState for " + topicName);
    }
    topicStoreVersionStateMap.put(topicName, record);
  }

  private synchronized void updateRemoteStorageMetadataService(IngestionStorageMetadata ingestionStorageMetadata) {
    byte[] content = serializeIngestionStorageMetadata(ingestionStorageMetadata);
    try {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.UPDATE_METADATA, content);
      FullHttpResponse response = client.sendRequest(httpRequest);
      if (response.status().equals(HttpResponseStatus.OK)) {
        byte[] responseContent = new byte[response.content().readableBytes()];
        response.content().readBytes(responseContent);
        IngestionTaskReport ingestionTaskReport = deserializeIngestionTaskReport(responseContent);
        if (logger.isDebugEnabled()) {
          logger.debug("Received ingestion task report response: " + ingestionTaskReport);
        }
      } else {
        logger.warn("Received bad ingestion task report response: " + response.status() + " for topic: " + ingestionStorageMetadata.topicName + ", partition: " + ingestionStorageMetadata.partitionId);
      }
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    } catch (Exception e) {
      /**
       * We only log the exception when failing to persist metadata updates into child process.
       * Child process might crashed, but it will be respawned and will be able to receive future updates.
       */
      logger.warn("Encounter exception when sending metadata updates to child process for topic: " + ingestionStorageMetadata.topicName + ", partition: " + ingestionStorageMetadata.partitionId, e);
    }
  }
}
