package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = HybridStoreConfigImpl.class)
public interface HybridStoreConfig extends DataModelBackedStructure<StoreHybridConfig> {
  long getRewindTimeInSeconds();

  long getOffsetLagThresholdToGoOnline();

  void setRewindTimeInSeconds(long rewindTimeInSeconds);

  void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline);

  long getProducerTimestampLagThresholdToGoOnlineInSeconds();

  DataReplicationPolicy getDataReplicationPolicy();

  void setDataReplicationPolicy(DataReplicationPolicy dataReplicationPolicy);

  BufferReplayPolicy getBufferReplayPolicy();

  HybridStoreConfig clone();

  default boolean isHybrid() {
    return this.getRewindTimeInSeconds() >= 0 && (this.getOffsetLagThresholdToGoOnline() >= 0
        || this.getProducerTimestampLagThresholdToGoOnlineInSeconds() >= 0);
  }
}
