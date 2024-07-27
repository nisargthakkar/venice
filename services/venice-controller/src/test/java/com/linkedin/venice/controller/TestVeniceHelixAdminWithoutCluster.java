package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminWithoutCluster {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*still exists in cluster.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenExistsInOtherCluster() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster("cluster2");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.of(storeConfig),
        Optional.empty(),
        Collections.emptySet(),
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*still exists in cluster.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenExistsInTheSameCluster() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Store store = TestUtils.getRandomStore();
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.of(store),
        Collections.emptySet(),
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  /**
   * Right now, version topic check is ignored since today, Venice is still keeping a couple of latest deprecated
   * version topics to avoid SNs failure due to UNKNOWN_TOPIC_OR_PARTITION errors.
   */
  @Test
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeVersionTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<PubSubTopic> topics = new HashSet<>();

    topics.add(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1)));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Topic.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenRTTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<PubSubTopic> topics = new HashSet<>();
    topics.add(pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(storeName)));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Topic.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<PubSubTopic> topics = new HashSet<>();
    topics.add(
        pubSubTopicRepository
            .getTopic(Utils.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName))));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Helix Resource.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreHelixResourceStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    List<String> resources = new LinkedList<>();
    resources.add(Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1));
    resources.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        Collections.emptySet(),
        resources,
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreHelixResourceStillExistsButHelixResourceSkipped() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    List<String> resources = new LinkedList<>();
    resources.add(Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1));
    resources.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        Collections.emptySet(),
        resources,
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName, false));
  }

  private void testCheckResourceCleanupBeforeStoreCreationWithParams(
      String clusterName,
      String storeName,
      Optional<StoreConfig> storeConfig,
      Optional<Store> store,
      Set<PubSubTopic> topics,
      List<String> helixResources,
      Consumer<VeniceHelixAdmin> testExecution) {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);

    ZkStoreConfigAccessor storeConfigAccessor = mock(ZkStoreConfigAccessor.class);
    doReturn(storeConfig.isPresent() ? storeConfig.get() : null).when(storeConfigAccessor).getStoreConfig(storeName);
    doReturn(storeConfigAccessor).when(admin).getStoreConfigAccessor(clusterName);

    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    doReturn(store.isPresent() ? store.get() : null).when(storeRepository).getStore(storeName);
    doReturn(storeRepository).when(admin).getMetadataRepository(clusterName);

    TopicManager topicManager = mock(TopicManager.class);
    doReturn(topics).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();
    doReturn(store.orElse(null)).when(admin).getStore(clusterName, storeName);

    doReturn(helixResources).when(admin).getAllLiveHelixResources(clusterName);

    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString());
    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString(), anyBoolean());
    doCallRealMethod().when(admin)
        .checkKafkaTopicAndHelixResource(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean());

    testExecution.accept(admin);
  }

  @Test
  public void testSourceRegionSelectionForTargetedRegionPush() {
    // cluster config setup
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    doReturn(config).when(multiClusterConfigs).getControllerConfig("test_cluster");
    doReturn("dc-4").when(config).getNativeReplicationSourceFabric();

    // store setup
    Store store = mock(Store.class);
    doReturn("dc-3").when(store).getNativeReplicationSourceFabric();

    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    doCallRealMethod().when(admin).getNativeReplicationSourceFabric(anyString(), any(), any(), any(), any());
    doReturn(multiClusterConfigs).when(admin).getMultiClusterConfigs();

    // Note that for some weird reasons, if this test case is moved below the store cannot return mocked response
    // even if the reference doesn't change.
    // store config (dc-3) is specified as 4th priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.empty(), Optional.empty(), null),
        "dc-3");

    // emergencySourceRegion (dc-0) is specified as 1st priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.of("dc-0"), "dc-1"),
        "dc-0");

    // VPJ plugin targeted region config (dc-1) is specified as 2nd priority
    doReturn(null).when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.empty(), "dc-1"),
        "dc-1");

    // VPJ source fabric (dc-2) is specified as 3rd priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.empty(), null),
        "dc-2");

    // cluster config (dc-4) is specified as 5th priority
    doReturn(null).when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.empty(), Optional.empty(), null),
        "dc-4");

    /**
     * When we have the following setup:
     * source fabric is dc-1,
     * store config is dc-3,
     * cluster config is dc-4,
     * targeted regions is dc-0, dc-2, dc-4, dc-99
     *
     * we should pick dc-4 as the source fabric even though it has lower priority than dc-3, but it's in the targeted list
     */
    doReturn("dc-3").when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric(
            "test_cluster",
            store,
            Optional.of("dc-1"),
            Optional.empty(),
            "dc-99, dc-0, dc-4, dc-2"),
        "dc-4");
  }
}
