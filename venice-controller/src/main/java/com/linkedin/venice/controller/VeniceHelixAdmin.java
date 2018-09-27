package com.linkedin.venice.controller;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.StoreStatusDecider;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.VeniceControllerConsumerFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.meta.*;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroGenericSerializer;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.utils.*;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;

import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;


/**
 * Helix Admin based on 0.6.6.4 APIs.
 *
 * <p>
 * After using controller as service mode. There are two levels of cluster and controllers. Each venice controller will
 * hold a level1 helix controller which will keep connecting to Helix, there is a cluster only used for all of these
 * level1 controllers(controller's cluster). The second level is our venice clusters. Like prod cluster, dev cluster
 * etc. Each of cluster will be Helix resource in the controller's cluster. Helix will choose one of level1 controller
 * becoming the master of our venice cluster. In our distributed controllers state transition handler, a level2 controller
 * will be initialized to manage this venice cluster only. If this level1 controller is chosen as the master controller
 * of multiple Venice clusters, multiple level2 controller will be created based on cluster specific config.
 * <p>
 * Admin is shared by multiple cluster's controllers running in one physical Venice controller instance.
 */
public class VeniceHelixAdmin implements Admin, StoreCleaner {
    private final VeniceControllerMultiClusterConfig multiClusterConfigs;
    private final String controllerClusterName;
    private final int controllerClusterReplica;
    private final String controllerName;
    private final String kafkaBootstrapServers;
    private final String kafkaSSLBootstrapServers;
    private final Map<String, AdminConsumerService> adminConsumerServices = new HashMap<>();
    // Track last exception when necessary
    private Map<String, Exception> lastExceptionMap = new ConcurrentHashMap<String, Exception>();

    public static final int CONTROLLER_CLUSTER_NUMBER_OF_PARTITION = 1;
    public static final long CONTROLLER_JOIN_CLUSTER_TIMEOUT_MS = 1000*300l; // 5min
    public static final long CONTROLLER_JOIN_CLUSTER_RETRY_DURATION_MS = 500l;

    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class);
    private static final int GET_PUSH_JOB_STATUS_RTT_MAX_RETRY_COUNT = 5;
    private static final long GET_PUSH_JOB_STATUS_RTT_RETRY_PERIOD_MS = 1000;

    private final HelixAdmin admin;
    private TopicManager topicManager;
    private final ZkClient zkClient;
    private final HelixAdapterSerializer adapterSerializer;
    private final ZkWhitelistAccessor whitelistAccessor;
    private final ExecutionIdAccessor executionIdAccessor;
    private final Optional<TopicReplicator> topicReplicator;
    private final long deprecatedJobTopicRetentionMs;
    private final long deprecatedJobTopicMaxRetentionMs;
    private final ZkStoreConfigAccessor storeConfigAccessor;
    protected final HelixReadOnlyStoreConfigRepository storeConfigRepo;
    private final VeniceWriterFactory veniceWriterFactory;
    private final VeniceControllerConsumerFactory veniceConsumerFactory;
    private final int minNumberOfUnusedKafkaTopicsToPreserve;
    private final int minNumberOfStoreVersionsToPreserve;
    private final StoreGraveyard storeGraveyard;

  /**
     * Level-1 controller, it always being connected to Helix. And will create sub-controller for specific cluster when
     * getting notification from Helix.
     */
    private SafeHelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data of level-1 cluster.
     */
    private PropertyKey.Builder level1KeyBuilder;

    private String pushJobStatusTopicName;

    private VeniceWriter<PushJobStatusRecordKey, PushJobStatusRecordValue> pushJobStatusWriter = null;

    private VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;
    //TODO Use different configs for different clusters when creating helix admin.
    public VeniceHelixAdmin(VeniceControllerMultiClusterConfig multiClusterConfigs, MetricsRepository metricsRepository) {
        this.multiClusterConfigs = multiClusterConfigs;
        VeniceControllerConfig commonConfig = multiClusterConfigs.getCommonConfig();
        this.controllerName = Utils.getHelixNodeIdentifier(multiClusterConfigs.getAdminPort());
        this.controllerClusterName = multiClusterConfigs.getControllerClusterName();
        this.controllerClusterReplica = multiClusterConfigs.getControllerClusterReplica();
        this.kafkaBootstrapServers = multiClusterConfigs.getKafkaBootstrapServers();
        this.kafkaSSLBootstrapServers = multiClusterConfigs.getSslKafkaBootstrapServers();
        this.deprecatedJobTopicRetentionMs = multiClusterConfigs.getDeprecatedJobTopicRetentionMs();
        this.deprecatedJobTopicMaxRetentionMs = multiClusterConfigs.getDeprecatedJobTopicMaxRetentionMs();

        this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
        this.minNumberOfStoreVersionsToPreserve = multiClusterConfigs.getMinNumberOfStoreVersionsToPreserve();

        // TODO: Re-use the internal zkClient for the ZKHelixAdmin and TopicManager.
        this.admin = new ZKHelixAdmin(multiClusterConfigs.getZkAddress());
        //There is no way to get the internal zkClient from HelixManager or HelixAdmin. So create a new one here.
        this.zkClient = new ZkClient(multiClusterConfigs.getZkAddress(), ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT);
        this.zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "controller-zk-client"));
        this.adapterSerializer = new HelixAdapterSerializer();
        veniceConsumerFactory = new VeniceControllerConsumerFactory(commonConfig);
        this.topicManager = new TopicManager(multiClusterConfigs.getKafkaZkAddress(),
            multiClusterConfigs.getTopicManagerKafkaOperationTimeOutMs(), veniceConsumerFactory);
        this.whitelistAccessor = new ZkWhitelistAccessor(zkClient, adapterSerializer);
        this.executionIdAccessor = new ZkExecutionIdAccessor(zkClient, adapterSerializer);
        this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer);
        this.storeConfigRepo = new HelixReadOnlyStoreConfigRepository(zkClient, adapterSerializer,
            commonConfig.getRefreshAttemptsForZkReconnect(), commonConfig.getRefreshIntervalForZkReconnectInMs());
        this.storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, multiClusterConfigs.getClusters());
        veniceWriterFactory = new VeniceWriterFactory(commonConfig.getProps().toProperties());
        this.topicReplicator =
            TopicReplicator.getTopicReplicator(topicManager, commonConfig.getProps(), veniceWriterFactory);

        // Create the parent controller and related cluster if required.
        createControllerClusterIfRequired();
        controllerStateModelFactory =
            new VeniceDistClusterControllerStateModelFactory(zkClient, adapterSerializer, this, metricsRepository);
        // Preset the configs for all known clusters.
        for (String cluster : multiClusterConfigs.getClusters()) {
            controllerStateModelFactory.addClusterConfig(cluster, multiClusterConfigs.getConfigForCluster(cluster));
        }
        // Initialized the helix manger for the level1 controller.
        initLevel1Controller();

        // Start store migration monitor background thread
        storeConfigRepo.refresh();
        startStoreMigrationMonitor();
    }

    private void initLevel1Controller() {
        manager = new SafeHelixManager(HelixManagerFactory
            .getZKHelixManager(controllerClusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT,
                multiClusterConfigs.getControllerClusterZkAddresss()));
        StateMachineEngine stateMachine = manager.getStateMachineEngine();
        stateMachine.registerStateModelFactory(LeaderStandbySMD.name, controllerStateModelFactory);
        try {
            manager.connect();
        } catch (Exception ex) {
            String errorMessage =
                " Error starting Helix controller cluster " + controllerClusterName + " controller " + controllerName;
            logger.error(errorMessage, ex);
            throw new VeniceException(errorMessage, ex);
        }
        level1KeyBuilder = new PropertyKey.Builder(manager.getClusterName());
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public ExecutionIdAccessor getExecutionIdAccessor() {
        return executionIdAccessor;
    }

    public HelixAdapterSerializer getAdapterSerializer() {
        return adapterSerializer;
    }

    @Override
    public synchronized void start(String clusterName) {
        //Simply validate cluster name here.
        clusterName = clusterName.trim();
        if (clusterName.startsWith("/") || clusterName.endsWith("/") || clusterName.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Invalid cluster name:" + clusterName);
        }
        createClusterIfRequired(clusterName);
        // The resource and partition may be disabled for this controller before, we need to enable again at first. Then the state transition will be triggered.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        admin.enablePartition(true, controllerClusterName, controllerName, clusterName, partitionNames);
        waitUnitControllerJoinsCluster(clusterName);
    }

    private void waitUnitControllerJoinsCluster(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
        long startTime = System.currentTimeMillis();
        try {
            while (!controllerStateModelFactory.hasJoinedCluster(clusterName)) {
                // Waiting time out
                if (System.currentTimeMillis() - startTime >= CONTROLLER_JOIN_CLUSTER_TIMEOUT_MS) {
                    throw new InterruptedException("Time out when waiting controller join cluster:" + clusterName);
                }
                // Check whether enough controller has been assigned.
                ExternalView externalView =
                    manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
                String partitionName = HelixUtils.getPartitionName(clusterName, 0);
                if (externalView != null && externalView.getStateMap(partitionName) != null) {
                    int assignedControllerCount = externalView.getStateMap(partitionName).size();
                    if (assignedControllerCount >= controllerClusterReplica) {
                        logger.info(
                            "Do not need to wait, this controller can not join because there is enough controller for cluster:"
                                + clusterName);
                        return;
                    }
                }
                // Wait
                Thread.sleep(CONTROLLER_JOIN_CLUSTER_RETRY_DURATION_MS);
            }
            logger.info("Controller joined the cluster:" + clusterName);
        } catch (InterruptedException e) {
            String errorMsg = "Controller can not join the cluster:" + clusterName;
            logger.error(errorMsg, e);
            throw new VeniceException(errorMsg, e);
        }
    }

    @Override
    public boolean isResourceStillAlive(String resourceName) {
        if (!Version.topicIsValidStoreVersion(resourceName)) {
            throw new VeniceException("Resource name: " + resourceName + " is invalid");
        }
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        // Find out the cluster first
        StoreConfig storeConfig = getStoreConfigAccessor().getStoreConfig(storeName);
        if (null == storeConfig) {
            logger.info("StoreConfig doesn't exist for store: " + storeName + ", will treat resource:" + resourceName + " as deprecated");
            return false;
        }
        String clusterName = storeConfig.getCluster();
        // Compose ZK path for external view of the resource
        String externalViewPath = "/" + clusterName + "/EXTERNALVIEW/" + resourceName;
        return zkClient.exists(externalViewPath);
    }

    @Override
    public boolean isClusterValid(String clusterName) {
        return admin.getClusters().contains(clusterName);
    }

    @Override
    public synchronized void addStore(String clusterName, String storeName, String owner, String keySchema,
        String valueSchema) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        logger.info("Start creating store: " + storeName);
        resources.lockForMetadataOperation();
        try{
            checkPreConditionForAddStore(clusterName, storeName, keySchema, valueSchema);
            VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
            Store newStore = new Store(storeName, owner, System.currentTimeMillis(), config.getPersistenceType(),
                config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy());
            HelixReadWriteStoreRepository storeRepo = resources.getMetadataRepository();
            storeRepo.lock();
            try {
                Store existingStore = storeRepo.getStore(storeName);
                if (existingStore != null) {
                    // We already check the pre-condition before, so if we could find a store with the same name,
                    // it means the store is a legacy store which is left by a failed deletion. So we should delete it.
                    deleteStore(clusterName, storeName, existingStore.getLargestUsedVersionNumber());
                }
                // Now there is not store exists in the store repository, we will try to retrieve the info from the graveyard.
                // Get the largestUsedVersionNumber from graveyard to avoid resource conflict.
                int largestUsedVersionNumber = storeGraveyard.getLargestUsedVersionNumber(storeName);
                newStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
                storeRepo.addStore(newStore);
                // Create global config for that store.
                storeConfigAccessor.createConfig(storeName, clusterName);
                logger.info("Store: " + storeName +
                    " has been created with largestUsedVersionNumber: " + newStore.getLargestUsedVersionNumber());
            } finally {
                storeRepo.unLock();
            }
            // Add schema
            HelixReadWriteSchemaRepository schemaRepo = resources.getSchemaRepository();
            schemaRepo.initKeySchema(storeName, keySchema);
            schemaRepo.addValueSchema(storeName, valueSchema, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
            logger.info("Completed creating Store: " + storeName);
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    public synchronized void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber) {
        checkControllerMastership(clusterName);
        logger.info("Start deleting store: " + storeName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try{
            HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
            storeRepository.lock();
            Store store = null;
            try {
                store = storeRepository.getStore(storeName);
                checkPreConditionForDeletion(clusterName, storeName, store);
                if (largestUsedVersionNumber == Store.IGNORE_VERSION) {
                    // ignore and use the local largest used version number.
                    logger.info("Give largest used version number is: " + largestUsedVersionNumber
                        + " will skip overwriting the local store.");
                } else if (largestUsedVersionNumber < store.getLargestUsedVersionNumber()) {
                    throw new VeniceException("Given largest used version number: " + largestUsedVersionNumber
                        + " is smaller than the largest used version number: " + store.getLargestUsedVersionNumber() +
                        " found in repository. Cluster: " + clusterName + ", store: " + storeName);
                } else {
                    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
                }

                String currentlyDiscoveredClusterName = storeConfigAccessor.getStoreConfig(storeName).getCluster(); // This is for store migration
                if (currentlyDiscoveredClusterName.equals(clusterName)) {
                    // Update deletion flag in Store to start deleting. In case of any failures during the deletion, the store
                    // could be deleted later after controller is recovered.
                    // A worse case is deletion succeeded in parent controller but failed in production controller. After skip
                    // the admin message offset, a store was left in some prod colos. While user re-create the store, we will
                    // delete this legacy store if isDeleting is true for this store.
                    StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
                    storeConfig.setDeleting(true);
                    storeConfigAccessor.updateConfig(storeConfig);
                } else {
                    // This is most likely the deletion after a store migration operation.
                    // In this case the storeConfig should not be deleted,
                    // because it is still being used to discover the cloned store
                    logger.warn("storeConfig for this store " + storeName
                        + " will not be deleted because it is currently pointing to another cluster: "
                        + currentlyDiscoveredClusterName);
                }
                storeRepository.updateStore(store);
            } finally {
                storeRepository.unLock();
            }
            // Delete All versions
            deleteAllVersionsInStore(clusterName, storeName);
            // Clean up topics
            truncateKafkaTopic(Version.composeRealTimeTopic(storeName));
            if (store != null) {
                truncateOldKafkaTopics(store, true);
            } else {
                // Defensive coding: This should never happen, unless someone adds a catch block to the above try/finally clause...
                logger.error("Unexpected null store instance...!");
            }
            // Move the store to graveyard. It will only re-create the znode for store's metadata excluding key and
            // value schemas.
            logger.info("Putting store:" + storeName + " into graveyard");
            storeGraveyard.putStoreIntoGraveyard(clusterName, storeRepository.getStore(storeName));
            // Helix will remove all data under this store's znode including key and value schemas.
            resources.getMetadataRepository().deleteStore(storeName);

            // Delete the config for this store after deleting the store.
            if (storeConfigAccessor.getStoreConfig(storeName).isDeleting()) {
                storeConfigAccessor.deleteConfig(storeName);
            }
            logger.info("Store " + storeName + " in cluster " + clusterName + " has been deleted.");
        } finally {
            resources.unlockForMetadataOperation();
        }
    }

    public synchronized void cloneStore(String srcClusterName, String destClusterName, StoreInfo srcStore,
        String keySchema, MultiSchemaResponse.Schema[] valueSchemas) {
        if (srcClusterName.equals(destClusterName)) {
            throw new VeniceException("Source cluster and destination cluster cannot be the same!");
        }

        String storeName = srcStore.getName();
        checkPreConditionForCloneStore(destClusterName, storeName);
        logger.info("Start cloning store: " + storeName);

        VeniceHelixResources resources = getVeniceHelixResource(destClusterName);
        HelixReadWriteSchemaRepository schemaRepo = resources.getSchemaRepository();
        HelixReadWriteStoreRepository storeRepo = resources.getMetadataRepository();

        // Create new store
        VeniceControllerClusterConfig config = getVeniceHelixResource(destClusterName).getConfig();
        Store clonedStore = new Store(storeName, srcStore.getOwner(), System.currentTimeMillis(), config.getPersistenceType(),
            config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy());

        storeRepo.lock();
        try {
            Store existingStore = storeRepo.getStore(storeName);
            if (existingStore != null) {
                throwStoreAlreadyExists(destClusterName, storeName);
            }
            storeRepo.addStore(clonedStore);
            logger.info("Cloned store " + storeName + " has been created with largestUsedVersionNumber "
                + clonedStore.getLargestUsedVersionNumber());

            //check store config
            if (!storeConfigAccessor.containsConfig(storeName)) {
                logger.warn("Expecting but cannot find storeConfig for this store " + storeName);
                storeConfigAccessor.createConfig(storeName, destClusterName);
            }
        } finally {
            storeRepo.unLock();
        }

        // Copy schemas
        synchronized (schemaRepo) {
            // Add key schema
            new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema); // Check whether key schema is valid. It should, because this is from an existing store.
            schemaRepo.initKeySchema(storeName, keySchema);

            // Add value schemas
            for (MultiSchemaResponse.Schema valueSchema : valueSchemas) {
                new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema.getSchemaStr()); // Check whether value schema is valid. It should, because this is from an existing store.
                schemaRepo.addValueSchema(storeName, valueSchema.getSchemaStr(), valueSchema.getId());
            }
        }

        // Copy remaining properties that will make the cloned store almost identical to the original
        UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore);
        this.updateStore(destClusterName, storeName, params);

        logger.info("Completed cloning Store: " + storeName);
    }

    public void sendPushJobStatusMessage(PushJobStatusRecordKey key, PushJobStatusRecordValue value) {
        if (multiClusterConfigs.getPushJobStatusStoreClusterName().isEmpty()
            || multiClusterConfigs.getPushJobStatusStoreName().isEmpty()) {
            // Push job status upload store is not configured.
            throw new VeniceException("Unable to upload the push job status because corresponding store is not configured");
        }
        if (pushJobStatusTopicName == null) {
            int attempts = 1;
            while (attempts <= GET_PUSH_JOB_STATUS_RTT_MAX_RETRY_COUNT) {
                // Since the push job status store is created asynchronously, it's possible that the store is not ready
                // yet. Therefore multiple attempts might be needed.
                try {
                    configurePushJobStatusTopic();
                    break;
                } catch (VeniceException e) {
                    logger.info("Get push job status rt topic attempts: "
                        + attempts + "/" + GET_PUSH_JOB_STATUS_RTT_MAX_RETRY_COUNT);
                }
                Utils.sleep(GET_PUSH_JOB_STATUS_RTT_RETRY_PERIOD_MS,
                    "The process of getting the push job status rt topic was interrupted");
                attempts++;
            }
            if (pushJobStatusTopicName == null) {
              throw new VeniceException("Unable to get the push job status rt topic");
            }
        }
      if (pushJobStatusWriter == null) {
          configurePushJobStatusWriter(key, value);
      }
      pushJobStatusWriter.put(key, value, multiClusterConfigs.getPushJobStatusValueSchemaId(), null);
    }

    private synchronized void configurePushJobStatusTopic() {
        if (pushJobStatusTopicName == null) {
            pushJobStatusTopicName = getRealTimeTopic(multiClusterConfigs.getPushJobStatusStoreClusterName(),
                multiClusterConfigs.getPushJobStatusStoreName());
        }
    }

    private synchronized void configurePushJobStatusWriter(PushJobStatusRecordKey key, PushJobStatusRecordValue value) {
        if (pushJobStatusWriter == null) {
            VeniceKafkaSerializer keySerializer = new VeniceAvroGenericSerializer(key.getSchema().toString());
            VeniceKafkaSerializer valueSerializer = new VeniceAvroGenericSerializer(value.getSchema().toString());
            pushJobStatusWriter = getVeniceWriterFactory()
                .getVeniceWriter(pushJobStatusTopicName, keySerializer, valueSerializer);
        }
    }

    public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
        //validate store and version exist
        Store store = getStore(clusterName, storeName);

        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }

        if (store.getCurrentVersion() == versionNumber){
            throw new VeniceHttpException(HttpStatus.SC_CONFLICT, "Cannot end push for version " + versionNumber + " that is currently being served");
        }

        if (!store.containsVersion(versionNumber)){
            throw new VeniceHttpException(HttpStatus.SC_NOT_FOUND, "Version " + versionNumber + " was not found for Store " + storeName
                + ".  Cannot end push for version that does not exist");
        }

        //write EOP message
        try (VeniceWriter writer = getVeniceWriterFactory()
            .getVeniceWriter(Version.composeKafkaTopic(storeName, versionNumber))) {
            if (alsoWriteStartOfPush) {
                writer.broadcastStartOfPush(new HashMap<>());
            }
            writer.broadcastEndOfPush(new HashMap<>());
        }
    }

    @Override
    public void migrateStore(String srcClusterName, String destClusterName, String storeName) {
        if (srcClusterName.equals(destClusterName)) {
            throw new VeniceException("Source cluster and destination cluster cannot be the same!");
        }

        // Get original store properties
        String srcControllerUrl = this.getMasterController(srcClusterName).getUrl(false);
        ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcControllerUrl);
        StoreInfo srcStore = srcControllerClient.getStore(storeName).getStore();
        String srcKeySchema = srcControllerClient.getKeySchema(storeName).getSchemaStr();
        MultiSchemaResponse.Schema[] srcValueSchemasResponse = srcControllerClient.getAllValueSchema(storeName).getSchemas();

        this.cloneStore(srcClusterName, destClusterName, srcStore, srcKeySchema, srcValueSchemasResponse);

        // Update migration src and dest cluster in storeConfig
        setStoreConfigForMigration(storeName, srcClusterName, destClusterName);

        // Set store migration flag for both original and cloned store
        UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStoreMigration(true);
        srcControllerClient.updateStore(storeName, params);     // update original store
        // Also decrease the largestUsedVersionNumber to trigger bootstrap
        params.setLargestUsedVersionNumber(0);
        this.updateStore(destClusterName, storeName, params);   // update cloned store
    }

    @Override
    public synchronized void updateClusterDiscovery(String storeName, String oldCluster, String newCluster) {
        StoreConfig storeConfig = this.storeConfigAccessor.getStoreConfig(storeName);
        if (storeConfig == null) {
            throw new VeniceException("Store config is empty!");
        } else if (!storeConfig.getCluster().equals(oldCluster)) {
            throw new VeniceException("Store " + storeName + " is expected to be in " + oldCluster + " cluster, but is actually in " + storeConfig.getCluster());
        }

        storeConfig.setCluster(newCluster);
        this.storeConfigAccessor.updateConfig(storeConfig);
        logger.info("Store " + storeName + " now belongs to cluster " + newCluster + " instead of " + oldCluster);
    }

    protected void checkPreConditionForCloneStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        checkStoreNameConflict(storeName);

        if (storeConfigAccessor.containsConfig(storeName)) {
            if (storeConfigAccessor.getStoreConfig(storeName).getCluster().equals(clusterName)) {
                // store exists in dest cluster
                throwStoreAlreadyExists(clusterName, storeName);
            }
        }
    }

    protected void checkPreConditionForAddStore(String clusterName, String storeName, String keySchema, String valueSchema) {
        checkControllerMastership(clusterName);
        checkStoreNameConflict(storeName);

        // Before creating store, check the global stores configs at first.
        // TODO As some store had already been created before we introduced global store config
        // TODO so we need a way to sync up the data. For example, while we loading all stores from ZK for a cluster,
        // TODO put them into global store configs.
        boolean isLagecyStore = false;
        if (storeConfigAccessor.containsConfig(storeName)) {
            // Controller was trying to delete the old store but failed.
            // Delete again before re-creating.
            // We lock the resource during deletion, so it's impossible to access the store which is being
            // deleted from here. So the only possible case is the deletion failed before.
            if (!storeConfigAccessor.getStoreConfig(storeName).isDeleting()) {
                throw new VeniceStoreAlreadyExistsException(storeName);
            } else {
                isLagecyStore = true;
            }
        }

        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        // If the store exists in store repository and it's still active(not being deleted), we don't allow to re-create it.
        if (store != null && !isLagecyStore) {
            throwStoreAlreadyExists(clusterName, storeName);
        }

        // Check whether the schema is valid or not
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
    }

    private void checkStoreNameConflict(String storeName) {
        if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
            throw new VeniceException("Store name: " + storeName + " clashes with the internal usage, please change it");
        }
    }

    protected Store checkPreConditionForDeletion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        checkPreConditionForDeletion(clusterName, storeName, store);
        return store;
    }

    private void checkPreConditionForDeletion(String clusterName, String storeName, Store store) {
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        if (store.isEnableReads() || store.isEnableWrites()) {
            String errorMsg = "Unable to delete the entire store or versions for store:" + storeName
                + ". Store has not been disabled. Both read and write need to be disabled before deleting.";
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
        }
    }

    protected Store checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, int versionNum) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
        return store;
    }

    private void checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, Store store, int versionNum) {
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        if (store.getCurrentVersion() == versionNum) {
            String errorMsg =
                "Unable to delete the version: " + versionNum + ". The current version could be deleted from store: "
                    + storeName;
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
        }
    }

    protected final static int VERSION_ID_UNSET = -1;

    @Override
    public synchronized Version addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition, int replicationFactor) {
        return addVersion(clusterName, storeName, Version.guidBasedDummyPushId(), versionNumber, numberOfPartition, replicationFactor, true, false);
    }

    /**
     * Note, versionNumber may be VERSION_ID_UNSET, which must be accounted for
     */
    protected synchronized Version addVersion(String clusterName, String storeName, String pushJobId, int versionNumber,
        int numberOfPartition, int replicationFactor, boolean whetherStartOfflinePush, boolean sendStartOfPush) {

        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        HelixReadWriteStoreRepository repository = resources.getMetadataRepository();

        Version version = null;
        OfflinePushStrategy strategy = null;

        try {
            resources.lockForMetadataOperation();
            try {
                repository.lock();
                int newTopicPartitionCount = 0;
                try {
                    Store store = repository.getStore(storeName);
                    if (store == null) {
                        throwStoreDoesNotExist(clusterName, storeName);
                    }
                    strategy = store.getOffLinePushStrategy();
                    if (versionNumber == VERSION_ID_UNSET) {
                        // No Version supplied, generate new version.
                        version = store.increaseVersion(pushJobId);
                    } else {
                        if (store.containsVersion(versionNumber)) {
                            throwVersionAlreadyExists(storeName, versionNumber);
                        }
                        version = new Version(storeName, versionNumber, pushJobId);
                        store.addVersion(version);
                    }
                    // Update default partition count if it have not been assigned.
                    if (store.getPartitionCount() == 0) {
                        store.setPartitionCount(
                            numberOfPartition); //TODO, persist numberOfPartitions at the version level
                    }
                    newTopicPartitionCount = store.getPartitionCount();
                    repository.updateStore(store);
                    logger.info("Add version:" + version.getNumber() + " for store:" + storeName);
                } finally {
                    repository.unLock();
                }

                VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
                createKafkaTopic(clusterName, version.kafkaTopicName(), newTopicPartitionCount,
                    clusterConfig.getKafkaReplicaFactor(), true);

                if (sendStartOfPush) {
                    // Note this will use default values for "sorted", "chunked" and "compressionStrategy" properties.
                    // TODO: add more flexibility and consider refactoring this entire logictus
                    veniceWriterFactory.getVeniceWriter(version.kafkaTopicName()).broadcastStartOfPush(new HashMap<>());
                }

                if (whetherStartOfflinePush) {
                    // TODO: there could be some problem here since topic creation is an async op, which means the new topic
                    // may not exist, When storage node is trying to consume the new created topic.

                    // We need to prepare to monitor before creating helix resource.
                    startMonitorOfflinePush(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor,
                        strategy);
                    createHelixResources(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor);
                }
            } finally {
                resources.unlockForMetadataOperation();
            }
            return version;
        } catch (Throwable e) {
            int failedVersionNumber = versionNumber;
            try {
                if (version != null) {
                    failedVersionNumber = version.getNumber();
                    String statusDetails = "Version creation failure, caught:\n" + ExceptionUtils.stackTraceToString(e);
                    handleVersionCreationFailure(clusterName, storeName, failedVersionNumber, statusDetails);
                }
            } catch (Throwable e1) {
                logger.error("Exception occured while handling version creation failure!", e1);
            } finally {
                throw new VeniceException(
                    "Failed to add a version: " + failedVersionNumber + " to store: " + storeName + " in cluster:"
                        + clusterName, e);
            }
        }
    }


    protected void handleVersionCreationFailure(String clusterName, String storeName, int versionNumber, String statusDetails){
        // Mark offline push job as Error and clean up resources because add version failed.
        OfflinePushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        offlinePushMonitor.markOfflinePushAsError(Version.composeKafkaTopic(storeName, versionNumber), statusDetails);
        deleteOneStoreVersion(clusterName, storeName, versionNumber);
    }

    @Override
    public synchronized Version incrementVersion(String clusterName, String storeName, int numberOfPartition,
        int replicationFactor) {
        return addVersion(clusterName, storeName, VERSION_ID_UNSET, numberOfPartition, replicationFactor);
    }

    /**
     * Note: this doesn't currently use the pushID to guarantee idempotence, unexpected behavior may result if multiple
     * batch jobs push to the same store at the same time.
     * TODO: refactor so that this method and the counterpart in {@link VeniceParentHelixAdmin} should have same behavior
     */
    @Override
    public synchronized Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
        int numberOfPartitions, int replicationFactor, boolean offlinePush, boolean isIncrementalPush,
        boolean sendStartOfPush) {

        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if (store == null) {
                throwStoreDoesNotExist(clusterName, storeName);
            }
            Optional<Version> existingVersionToUse = getVersionWithPushId(store, pushJobId);
            if (existingVersionToUse.isPresent()) {
                return existingVersionToUse.get();
            }
        } finally {
            repository.unLock();
        }

        return isIncrementalPush ? getIncrementalPushVersion(clusterName, storeName)
            : addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor,
                offlinePush, sendStartOfPush);
    }

    /**
     * The intended semantic is to use this method to find the version that something is currently pushing to.  It looks
     * at all versions greater than the current version and identifies the version with a status of STARTED.  If there
     * is no STARTED version, it creates a new one for the push to use.  This means we cannot use this method to support
     * multiple concurrent pushes.
     *
     * @param store
     * @return the started version if there is only one, throws an exception if there is an error version with
     * a greater number than the current version.  Otherwise returns Optional.empty()
     */
    protected static Optional<Version> getStartedVersion(Store store){
        List<Version> startedVersions = new ArrayList<>();
        for (Version version : store.getVersions()) {
            if (version.getNumber() <= store.getCurrentVersion()) {
                continue;
            }
            switch (version.getStatus()) {
                case ONLINE:
                case PUSHED:
                    break; // These we can ignore
                case STARTED:
                    startedVersions.add(version);
                    break;
                case ERROR:
                case NOT_CREATED:
                default:
                    throw new VeniceException("Version " + version.getNumber() + " for store " + store.getName()
                        + " has status " + version.getStatus().toString() + ".  Cannot create a new version until this store is cleaned up.");
            }
        }
        if (startedVersions.size() == 1){
            return Optional.of(startedVersions.get(0));
        } else if (startedVersions.size() > 1) {
            String startedVersionsString = startedVersions.stream().map(Version::getNumber).map(n -> Integer.toString(n)).collect(Collectors.joining(","));
            throw new VeniceException("Store " + store.getName() + " has versions " + startedVersionsString + " that are all STARTED.  "
                + "Cannot create a new version while there are multiple STARTED versions");
        }
        return Optional.empty();
    }

    /**
     *
     * @param store
     * @param pushId
     * @return
     */
    protected static Optional<Version> getVersionWithPushId(Store store, String pushId){
        for (Version version : store.getVersions()) {
            if (version.getPushJobId().equals(pushId)) {
                logger.info("Version request for pushId " + pushId + " and store " + store.getName()
                    + ".  pushId already exists, so returning existing version " + version.getNumber());
                return Optional.of(version); // Early exit
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized String getRealTimeTopic(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        Set<String> currentTopics = topicManager.listTopics();
        String realTimeTopic = Version.composeRealTimeTopic(storeName);
        if (currentTopics.contains(realTimeTopic)){
            return realTimeTopic;
        } else {
            HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
            repository.lock();
            try {
                Store store = repository.getStore(storeName);
                if (store == null) {
                    throwStoreDoesNotExist(clusterName, storeName);
                }
                if (!store.isHybrid()){
                    logAndThrow("Store " + storeName + " is not hybrid, refusing to return a realtime topic");
                }
                int partitionCount = store.getPartitionCount();
                if (partitionCount == 0){
                    //TODO:  partitioning is currently decided on first version push, and we need to match that versioning
                    // we should evaluate alternatives such as allowing the RT topic request to initialize the number of
                    // partitions, or setting the number of partitions at store creation time instead of at first version
                    throw new VeniceException("Store: " + storeName + " is not initialized with a version yet");
                }
                VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
                createKafkaTopic(clusterName, realTimeTopic, partitionCount, clusterConfig.getKafkaReplicaFactor(), false);
                //TODO: if there is an online version from a batch push before this store was hybrid then we won't start
                // replicating to it.  A new version must be created.
                logger.warn("Creating real time topic per topic request for store " + storeName + ".  "
                  + "Buffer replay wont start for any existing versions");
            } finally {
                repository.unLock();
            }
            return realTimeTopic;
        }
    }

    @Override
    public synchronized Version getIncrementalPushVersion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if (store == null) {
                throwStoreDoesNotExist(clusterName, storeName);
            }

            if (!store.isIncrementalPushEnabled()) {
                throw new VeniceException("Incremental push is not enabled for store: " + storeName);
            }

            List<Version> versions = store.getVersions();
            if (versions.isEmpty()) {
                throw new VeniceException("Store: " + storeName + " is not initialized with a version yet");
            }

            /**
             * Don't use {@link Store#getCurrentVersion()} here since it is always 0 in parent controller
             */
            Version version = versions.get(versions.size() - 1);
            if (version.getStatus() == VersionStatus.ERROR) {
                throw new VeniceException("cannot have incremental push because current version is in error status. "
                    + "Version: " + version.getNumber() + " Store:" + storeName);
            }
            return version;
        } finally {
            repository.unLock();
        }
    }

    @Override
    public int getCurrentVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        if (store.isEnableReads()) {
            return store.getCurrentVersion();
        } else {
            return Store.NON_EXISTING_VERSION;
        }
    }

    @Override
    public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
        return null;
    }

    @Override
    public Version peekNextVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        Version version = store.peekNextVersion(); /* Does not modify the store */
        logger.info("Next version would be: " + version.getNumber() + " for store: " + storeName);
        return version;
    }

    @Override
    public synchronized List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {

            HelixReadWriteStoreRepository repository = resources.getMetadataRepository();
            List<Version> deletingVersionSnapshot = new ArrayList<>();
            repository.lock();
            try {
                Store store = repository.getStore(storeName);
                checkPreConditionForDeletion(clusterName, storeName, store);
                logger.info("Deleting all versions in store: " + store + " in cluster: " + clusterName);
                // Set current version to NON_VERSION_AVAILABLE. Otherwise after this store is enabled again, as all of
                // version were deleted, router will get a current version which does not exist actually.
                store.setEnableWrites(true);
                store.setCurrentVersion(Store.NON_EXISTING_VERSION);
                store.setEnableWrites(false);
                repository.updateStore(store);
                deletingVersionSnapshot = new ArrayList<>(store.getVersions());
            } finally {
                repository.unLock();
            }
            // Do not lock the entire deleting block, because during the deleting, controller would acquire repository lock
            // to query store when received the status update from storage node.
            for (Version version : deletingVersionSnapshot) {
                deleteOneStoreVersion(clusterName, version.getStoreName(), version.getNumber());
            }
            logger.info("Deleted all versions in store: " + storeName + " in cluster: " + clusterName);
            return deletingVersionSnapshot;
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            HelixReadWriteStoreRepository repository = resources.getMetadataRepository();
            Store store = repository.getStore(storeName);
            // Here we do not require the store be disabled. So it might impact reads
            // The thing is a new version is just online, now we delete the old version. So some of routers
            // might still use the old one as the current version, so when they send the request to that version,
            // they will get error response.
            // TODO the way to solve this could be: Introduce a timestamp to represent when the version is online.
            // TOOD And only allow to delete the old version that the newer version has been online for a while.
            checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
            if (!store.containsVersion(versionNum)) {
                logger.warn(
                    "Ignore the deletion request. Could not find version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
                return;
            }
            logger.info("Deleting version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
            deleteOneStoreVersion(clusterName, storeName, versionNum);
            logger.info("Deleted version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    /**
     * Delete version from cluster, removing all related resources
     */
    @Override
    public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
            logger.info("Deleting helix resource:" + resourceName + " in cluster:" + clusterName);
            deleteHelixResource(clusterName, resourceName);
            logger.info("Killing offline push for:" + resourceName + " in cluster:" + clusterName);
            killOfflinePush(clusterName, resourceName);

            Store store = getVeniceHelixResource(clusterName).getMetadataRepository().getStore(storeName);
            if (topicReplicator.isPresent()) {
                // Do not delete topic replicator during store migration
                // In such case, the topic replicator will be deleted after store migration, triggered by a new push job
                if (!store.isMigrating()) {
                    String realTimeTopic = Version.composeRealTimeTopic(storeName);
                    topicReplicator.get().terminateReplication(realTimeTopic, resourceName);
                }
            }
            Optional<Version> deletedVersion = deleteVersionFromStoreRepository(clusterName, storeName, versionNumber);
            if (deletedVersion.isPresent()) {
                // Do not delete topic during store migration
                // In such case, the topic will be deleted after store migration, triggered by a new push job
                if (!store.isMigrating()) {
                    truncateKafkaTopic(deletedVersion.get().kafkaTopicName());
                }
            }
            stopMonitorOfflinePush(clusterName, resourceName);
        } finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    public void retireOldStoreVersions(String clusterName, String storeName) {
        logger.info("Retiring old versions for store: " + storeName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
            Store store = storeRepository.getStore(storeName);
            List<Version> versionsToDelete = store.retrieveVersionsToDelete(minNumberOfStoreVersionsToPreserve);
            for (Version version : versionsToDelete) {
                deleteOneStoreVersion(clusterName, storeName, version.getNumber());
                logger.info("Retired store:" + store.getName() + " version:" + version.getNumber());
            }
            logger.info("Retired " + versionsToDelete.size() + " versions for store: " + storeName);

            truncateOldKafkaTopics(store, false);
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    /***
     * Delete the version specified from the store and return the deleted version.
     */
    protected Optional<Version> deleteVersionFromStoreRepository(String clusterName, String storeName, int versionNumber) {
        HelixReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
        logger.info("Deleting version " + versionNumber + " in Store:" + storeName + " in cluster:" + clusterName);
        Version deletedVersion = null;
        storeRepository.lock();
        try {
            Store store = storeRepository.getStore(storeName);
            if (store == null) {
                throw new VeniceNoStoreException(storeName);
            }
            deletedVersion = store.deleteVersion(versionNumber);
            if (deletedVersion == null) {
                logger.warn("Can not find version: " + versionNumber + " in store: " + storeName + ".  It has probably already been deleted");
            }
            storeRepository.updateStore(store);
        } finally {
            storeRepository.unLock();
        }
        logger.info("Deleted version " + versionNumber + " in Store: " + storeName + " in cluster: " + clusterName);
        if (null == deletedVersion) {
            return Optional.empty();
        } else {
            return Optional.of(deletedVersion);
        }
    }

    @Override
    public boolean isTopicTruncated(String kafkaTopicName) {
        return getTopicManager().isTopicTruncated(kafkaTopicName, deprecatedJobTopicMaxRetentionMs);
    }

    @Override
    public boolean isTopicTruncatedBasedOnRetention(long retention) {
        return getTopicManager().isRetentionBelowTruncatedThreshold(retention, deprecatedJobTopicMaxRetentionMs);
    }

    @Override
    public void truncateKafkaTopic(String kafkaTopicName) {
        if (getTopicManager().containsTopic(kafkaTopicName)) {
            getTopicManager().updateTopicRetention(kafkaTopicName, deprecatedJobTopicRetentionMs);
            logger.info("Updated topic: " + kafkaTopicName + " with retention.ms: " + deprecatedJobTopicRetentionMs);
        } else {
            logger.info("Topic: " + kafkaTopicName + " doesn't exist, will skip the truncation");
        }
    }

    /**
     * Truncate old unused Kafka topics. These could arise from resource leaking of topics during certain
     * scenarios.
     *
     * If it is for store deletion, this function will truncate all the topics;
     * Otherwise, it will only truncate topics without corresponding active version.
     *
     * N.B.: visibility is package-private to ease testing...
     *
     * @param store for which to clean up old topics
     * @param forStoreDeletion
     *
     */
    void truncateOldKafkaTopics(Store store, boolean forStoreDeletion) {
        if (store.isMigrating())  {
            logger.info("This store " + store.getName() + " is being migrated. Skip topic deletion.");
            return;
        }

        Set<Integer> currentlyKnownVersionNumbers = store.getVersions().stream()
            .map(version -> version.getNumber())
            .collect(Collectors.toSet());

        Set<String> allTopics = topicManager.listTopics();
        List<String> allTopicsRelatedToThisStore = allTopics.stream()
            /** Exclude RT buffer topics, admin topics and all other special topics */
            .filter(t -> Version.topicIsValidStoreVersion(t))
            /** Keep only those topics pertaining to the store in question */
            .filter(t -> Version.parseStoreFromKafkaTopicName(t).equals(store.getName()))
            .collect(Collectors.toList());

        if (allTopicsRelatedToThisStore.isEmpty()) {
            logger.info("Searched for old topics belonging to store '" + store.getName() + "', and did not find any.");
            return;
        }
        List<String> oldTopicsToTruncate = allTopicsRelatedToThisStore;
        if (!forStoreDeletion) {
            // For store version deprecation, controller will truncate all the topics without corresponding versions.
            oldTopicsToTruncate = allTopicsRelatedToThisStore.stream().
                filter((topic) -> !currentlyKnownVersionNumbers.contains(Version.parseVersionFromKafkaTopicName(topic)))
                .collect(Collectors.toList());
        }

        if (oldTopicsToTruncate.isEmpty()) {
            logger.info("Searched for old topics belonging to store '" + store.getName() + "', and did not find any.");
        } else {
            logger.info("Detected the following old topics to truncate: " + String.join(", ", oldTopicsToTruncate));
            oldTopicsToTruncate.stream()
                .forEach(t -> truncateKafkaTopic(t));
            logger.info("Finished truncating old topics for store '" + store.getName() + "'.");
        }
    }

    @Override
    public void updatePushProperties(String cluster, String storeName, int version, Map<String, String> properties) {
        throw new VeniceUnsupportedOperationException("update push properties only supports in the paraent admin.");
    }


    /***
     * If you need to do mutations on the store, then you must hold onto the lock until you've persisted your mutations.
     * Only use this method if you're doing read-only operations on the store.
     * @param clusterName
     * @param storeName
     * @return
     */
    private Store getStoreForReadOnly(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            return store; /* is a clone */
        } finally {
            repository.unLock();
        }
    }

    @Override
    public List<Version> versionsForStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        List<Version> versions;
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            versions = store.getVersions();
        } finally {
            repository.unLock();
        }
        return versions;
    }

    @Override
    public List<Store> getAllStores(String clusterName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            return repository.listStores();
        } finally {
            repository.unLock();
        }
    }

    @Override
    public Map<String, String> getAllStoreStatuses(String clusterName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            List<Store> storeList = repository.listStores();
            RoutingDataRepository routingDataRepository =
                getVeniceHelixResource(clusterName).getRoutingDataRepository();
            ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
            return StoreStatusDecider.getStoreStatues(storeList, resourceAssignment,
                getVeniceHelixResource(clusterName).getConfig());
        } finally {
            repository.unLock();
        }
    }

    @Override
    public boolean hasStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            return repository.hasStore(storeName);
        } finally {
            repository.unLock();
        }
    }

    @Override
    public Store getStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        return repository.getStore(storeName);
    }

    @Override
    public synchronized void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber){
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.getCurrentVersion() != Store.NON_EXISTING_VERSION) {
                if (!store.containsVersion(versionNumber)) {
                    throw new VeniceException("Version:" + versionNumber + " does not exist for store:" + storeName);
                }

                if (!store.isEnableWrites()) {
                    throw new VeniceException("Unable to update store:" + storeName + " current version since store writeability is false");
                }
            }
            store.setCurrentVersion(versionNumber);

            return store;
        });
    }

    @Override
    public synchronized void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLargestUsedVersionNumber(versionNumber);
            return store;
        });
    }

    @Override
    public synchronized void setStoreOwner(String clusterName, String storeName, String owner) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setOwner(owner);
            return store;
        });
    }

    /**
     * Since partition check/calculation only happens when adding new store version, {@link #setStorePartitionCount(String, String, int)}
     * would only change the number of partition for the following pushes. Current version would not be changed.
     */
    @Override
    public synchronized void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
        VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.getPartitionCount() != partitionCount && store.isHybrid()) {
                throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Cannot change partition count for a hybrid store");
            } else {
                int desiredPartitionCount = partitionCount;

                if (desiredPartitionCount > clusterConfig.getMaxNumberOfPartition()) {
                    desiredPartitionCount = clusterConfig.getMaxNumberOfPartition();
                } else if (desiredPartitionCount < clusterConfig.getNumberOfPartition()) {
                    desiredPartitionCount = clusterConfig.getNumberOfPartition();
                }

                store.setPartitionCount(desiredPartitionCount);
                return store;
            }
        });
    }

    @Override
    public synchronized void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableWrites(desiredWriteability);

            return store;
        });
    }

    @Override
    public synchronized void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableReads(desiredReadability);

            return store;
        });
    }

    @Override
    public synchronized void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableReads(isAccessible);
            store.setEnableWrites(isAccessible);

            return store;
        });
    }

    /**
     * We will not expose this interface to Spark server. Updating quota can only be done by #updateStore
     * TODO: remove all store attribute setters.
     */
    private synchronized void setStoreStorageQuota(String clusterName, String storeName, long storageQuotaInByte) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (storageQuotaInByte < 0 && storageQuotaInByte != Store.UNLIMITED_STORAGE_QUOTA) {
                throw new VeniceException("storage quota can not be less than 0");
            }
            store.setStorageQuotaInByte(storageQuotaInByte);

            return store;
        });
    }

    private synchronized void setStoreReadQuota(String clusterName, String storeName, long readQuotaInCU) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (readQuotaInCU < 0) {
                throw new VeniceException("read quota can not be less than 0");
            }
            store.setReadQuotaInCU(readQuotaInCU);

            return store;
        });
    }

    public synchronized void setAccessControl(String clusterName, String storeName, boolean accessControlled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setAccessControlled(accessControlled);

            return store;
        });
    }

    public synchronized  void setStoreCompressionStrategy(String clusterName, String storeName,
                                                          CompressionStrategy compressionStrategy) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setCompressionStrategy(compressionStrategy);

            return store;
        });
    }

    public synchronized  void setChunkingEnabled(String clusterName, String storeName,
        boolean chunkingEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setChunkingEnabled(chunkingEnabled);

            return store;
        });
    }

    public synchronized void setIncrementalPushEnabled(String clusterName, String storeName,
        boolean incrementalPushEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (incrementalPushEnabled && store.isHybrid()) {
                throw new VeniceException("hybrid store doesn't support incremental push");
            }
            store.setIncrementalPushEnabled(incrementalPushEnabled);

            return  store;
        });
    }

    public synchronized  void setSingleGetRouterCacheEnabled(String clusterName, String storeName,
        boolean singleGetRouterCacheEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setSingleGetRouterCacheEnabled(singleGetRouterCacheEnabled);

            return store;
        });
    }

    public synchronized void setBatchGetRouterCacheEnabled(String clusterName, String storeName,
        boolean batchGetRouterCacheEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBatchGetRouterCacheEnabled(batchGetRouterCacheEnabled);
            return store;
        });
    }

    public synchronized  void setBatchGetLimit(String clusterName, String storeName,
        int batchGetLimit) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBatchGetLimit(batchGetLimit);

            return store;
        });
    }

    public synchronized  void setNumVersionsToPreserve(String clusterName, String storeName,
        int numVersionsToPreserve) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setNumVersionsToPreserve(numVersionsToPreserve);

            return store;
        });
    }

    public synchronized void setStoreMigration(String clusterName, String storeName, boolean migrating) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setMigrating(migrating);
            return store;
        });
    }

    /**
     * This function will check whether the store update will cause the case that a hybrid or incremental push store will have router-cache enabled
     * or a compressed store will have router-cache enabled.
     *
     * For now, router cache shouldn't be enabled for a hybrid store.
     * TODO: need to remove this check when the proper fix (cross-colo race condition) is implemented.
     *
     * Right now, this function doesn't check whether the hybrid/incremental push config and router-cache flag will be updated at the same time:
     * 1. Current store originally is a hybrid/incremental push store;
     * 2. The update operation will turn this store to be a batch-only store, and enable router-cache at the same time;
     * The reason not to check the above scenario is that the hybrid config/router-cache update is not atomic, so admin should
     * update the store to be batch-only store first and turn on the router-cache feature after.
     *
     * BTW, it seems no way to update a hybrid store to be a batch-only store.
     *
     * @param store
     * @param newIncrementalPushConfig
     * @param newHybridStoreConfig
     * @param newSingleGetRouterCacheEnabled
     * @param newBatchGetRouterCacheEnabled
     */
    protected void checkWhetherStoreWillHaveConflictConfigForCaching(Store store,
        Optional<Boolean> newIncrementalPushConfig,
        Optional<HybridStoreConfig> newHybridStoreConfig,
        Optional<Boolean> newSingleGetRouterCacheEnabled,
        Optional<Boolean> newBatchGetRouterCacheEnabled) {
        String storeName = store.getName();
        if ((store.isHybrid() || store.isIncrementalPushEnabled()) && (newSingleGetRouterCacheEnabled.orElse(false) || newBatchGetRouterCacheEnabled.orElse(false))) {
            throw new VeniceException("Router cache couldn't be enabled for store: " + storeName + " since it is a hybrid/incremental push store");
        }
        if ((store.isSingleGetRouterCacheEnabled() || store.isBatchGetRouterCacheEnabled()) && (newHybridStoreConfig.isPresent() || newIncrementalPushConfig.orElse(false))) {
            throw new VeniceException("Hybrid/incremental push couldn't be enabled for store: " + storeName + " since it enables router-cache");
        }
    }

    @Override
    public synchronized void updateStore(
        String clusterName,
        String storeName,
        Optional<String> owner,
        Optional<Boolean> readability,
        Optional<Boolean> writeability,
        Optional<Integer> partitionCount,
        Optional<Long> storageQuotaInByte,
        Optional<Long> readQuotaInCU,
        Optional<Integer> currentVersion,
        Optional<Integer> largestUsedVersionNumber,
        Optional<Long> hybridRewindSeconds,
        Optional<Long> hybridOffsetLagThreshold,
        Optional<Boolean> accessControlled,
        Optional<CompressionStrategy> compressionStrategy,
        Optional<Boolean> chunkingEnabled,
        Optional<Boolean> singleGetRouterCacheEnabled,
        Optional<Boolean> batchGetRouterCacheEnabled,
        Optional<Integer> batchGetLimit,
        Optional<Integer> numVersionsToPreserve,
        Optional<Boolean> incrementalPushEnabled,
        Optional<Boolean> storeMigration) {
        Store originalStoreToBeCloned = getStore(clusterName, storeName);
        if (null == originalStoreToBeCloned) {
            throw new VeniceException("The store '" + storeName + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
        }
        Store originalStore = originalStoreToBeCloned.cloneStore();

        if (originalStore.isMigrating()) {
            if (!(storeMigration.isPresent() || readability.isPresent() || writeability.isPresent())) {
                String errMsg = "This update operation is not allowed during store migration!";
                logger.warn(errMsg + " Store name: " + storeName);
                throw new VeniceException(errMsg);
            }
        }

        Optional<HybridStoreConfig> hybridStoreConfig = Optional.empty();
        if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent()) {
            HybridStoreConfig hybridConfig = mergeNewSettingsIntoOldHybridStoreConfig(
                originalStore, hybridRewindSeconds, hybridOffsetLagThreshold);
            if (null != hybridConfig) {
                hybridStoreConfig = Optional.of(hybridConfig);
            }
        }

        checkWhetherStoreWillHaveConflictConfigForCaching(originalStore, incrementalPushEnabled,hybridStoreConfig, singleGetRouterCacheEnabled, batchGetRouterCacheEnabled);

        try {
            if (owner.isPresent()) {
                setStoreOwner(clusterName, storeName, owner.get());
            }

            if (readability.isPresent()) {
                setStoreReadability(clusterName, storeName, readability.get());
            }

            if (writeability.isPresent()) {
                setStoreWriteability(clusterName, storeName, writeability.get());
            }

            if (partitionCount.isPresent()) {
                setStorePartitionCount(clusterName, storeName, partitionCount.get());
            }

            if (storageQuotaInByte.isPresent()) {
                setStoreStorageQuota(clusterName, storeName, storageQuotaInByte.get());
            }

            if (readQuotaInCU.isPresent()) {
                setStoreReadQuota(clusterName, storeName, readQuotaInCU.get());
            }

            if (currentVersion.isPresent()) {
                setStoreCurrentVersion(clusterName, storeName, currentVersion.get());
            }

            if (largestUsedVersionNumber.isPresent()) {
                setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersionNumber.get());
            }

            if (hybridStoreConfig.isPresent()) {
                // To fix the final variable problem in the lambda expression
                final HybridStoreConfig finalHybridConfig = hybridStoreConfig.get();
                storeMetadataUpdate(clusterName, storeName, store -> {
                    if (store.isIncrementalPushEnabled()) {
                        throw new VeniceException("incremental push store could not support hybrid");
                    }
                    store.setHybridStoreConfig(finalHybridConfig);
                    return store;
                });
            }

            if (singleGetRouterCacheEnabled.isPresent()) {
                setSingleGetRouterCacheEnabled(clusterName, storeName, singleGetRouterCacheEnabled.get());
            }

            if (batchGetRouterCacheEnabled.isPresent()) {
              setBatchGetRouterCacheEnabled(clusterName, storeName, batchGetRouterCacheEnabled.get());
            }

            if (accessControlled.isPresent()) {
                setAccessControl(clusterName, storeName, accessControlled.get());
            }

            if (compressionStrategy.isPresent()) {
                setStoreCompressionStrategy(clusterName, storeName, compressionStrategy.get());
            }

            if (chunkingEnabled.isPresent()) {
                setChunkingEnabled(clusterName, storeName, chunkingEnabled.get());
            }

            if (batchGetLimit.isPresent()) {
                setBatchGetLimit(clusterName, storeName, batchGetLimit.get());
            }

            if (numVersionsToPreserve.isPresent()) {
                setNumVersionsToPreserve(clusterName, storeName, numVersionsToPreserve.get());
            }

            if (incrementalPushEnabled.isPresent()) {
                setIncrementalPushEnabled(clusterName, storeName, incrementalPushEnabled.get());
            }

            if (storeMigration.isPresent()) {
                setStoreMigration(clusterName, storeName, storeMigration.get());
            }
            logger.info("Finished updating store: " + storeName + " in cluster: " + clusterName);
        } catch (VeniceException e) {
            logger.error("Caught exception during update to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will attempt to rollback changes.", e);
            //rollback to original store
            storeMetadataUpdate(clusterName, storeName, store -> originalStore);
            logger.error("Successfully rolled back changes to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will now throw the original exception (" + e.getClass().getSimpleName() + ").");
            throw e;
        }
    }

    /**
     * Used by both the {@link VeniceHelixAdmin} and the {@link VeniceParentHelixAdmin}
     *
     * @param oldStore Existing Store that is the source for updates. This object will not be modified by this method.
     * @param hybridRewindSeconds Optional is present if the returned object should include a new rewind time
     * @param hybridOffsetLagThreshold Optional is present if the returned object should include a new offset lag threshold
     * @return null if oldStore has no hybrid configs and optionals are not present,
     *   otherwise a fully specified {@link HybridStoreConfig}
     */
    protected static HybridStoreConfig mergeNewSettingsIntoOldHybridStoreConfig(Store oldStore,
            Optional<Long> hybridRewindSeconds, Optional<Long> hybridOffsetLagThreshold){
        if (!hybridRewindSeconds.isPresent() && !hybridOffsetLagThreshold.isPresent() && !oldStore.isHybrid()){
            return null; //For the nullable union in the avro record
        }
        HybridStoreConfig hybridConfig;
        if (oldStore.isHybrid()){ // for an existing hybrid store, just replace any specified values
            HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
            hybridConfig = new HybridStoreConfig(
                hybridRewindSeconds.isPresent()
                    ? hybridRewindSeconds.get()
                    : oldHybridConfig.getRewindTimeInSeconds(),
                hybridOffsetLagThreshold.isPresent()
                    ? hybridOffsetLagThreshold.get()
                    : oldHybridConfig.getOffsetLagThresholdToGoOnline()
            );
        } else { // switching a non-hybrid store to hybrid; must specify every value
            if (!(hybridRewindSeconds.isPresent() && hybridOffsetLagThreshold.isPresent())) {
                throw new VeniceException(oldStore.getName() + " was not a hybrid store.  In order to make it a hybrid store both "
                    + " rewind time in seconds and offset lag threshold must be specified");
            }
            hybridConfig = new HybridStoreConfig(
                hybridRewindSeconds.get(),
                hybridOffsetLagThreshold.get()
            );
        }
        return hybridConfig;
    }

    public void storeMetadataUpdate(String clusterName, String storeName, StoreMetadataOperation operation) {
        checkPreConditionForUpdateStore(clusterName, storeName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            repository.updateStore(operation.update(store));
        } catch (Exception e) {
            logger.error("Failed to execute StoreMetadataOperation.", e);
            throw e;
        } finally {
            repository.unLock();
        }
    }

    protected void checkPreConditionForUpdateStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        if (repository.getStore(storeName) == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
    }

    @Override
    public double getStorageEngineOverheadRatio(String clusterName) {
        return multiClusterConfigs.getConfigForCluster(clusterName).getStorageEngineOverheadRatio();
    }

    // TODO: Though controller can control, multiple Venice-clusters, kafka topic name needs to be unique
    // among them. If there the same store name is present in two different venice clusters, the code
    // will fail and might exhibit other issues.
    private void createKafkaTopic(String clusterName, String kafkaTopic, int numberOfPartition, int kafkaReplicaFactor, boolean eternal) {
        checkControllerMastership(clusterName);
        topicManager.createTopic(kafkaTopic, numberOfPartition, kafkaReplicaFactor, eternal);
    }

    private void createHelixResources(String clusterName, String kafkaTopic , int numberOfPartition , int replicationFactor)
        throws InterruptedException {
        if (!admin.getResourcesInCluster(clusterName).contains(kafkaTopic)) {
            admin.addResource(clusterName, kafkaTopic, numberOfPartition,
                VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.toString(),
                AutoRebalanceStrategy.class.getName());
            VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
            IdealState idealState = admin.getResourceIdealState(clusterName, kafkaTopic);
            // We don't set the delayed time per resoruce, we will use the cluster level helix config to decide
            // the delayed reblance time
            idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
            idealState.setMinActiveReplicas(config.getMinActiveReplica());
            idealState.setRebalanceStrategy(config.getHelixRebalanceAlg());
            admin.setResourceIdealState(clusterName, kafkaTopic, idealState);
            logger.info("Enabled delayed re-balance for resource:" + kafkaTopic);
            admin.rebalance(clusterName, kafkaTopic, replicationFactor);
            logger.info("Added " + kafkaTopic + " as a resource to cluster: " + clusterName);
            // TODO Wait until there are enough nodes assigned to the given resource.
            // This waiting is not mandatory in our new offline push monitor. But in order to keep the logic as similar
            // as before, we still waiting here. This logic could be removed later.
            OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
            monitor.waitUntilNodesAreAssignedForResource(kafkaTopic, config.getOffLineJobWaitTimeInMilliseconds());
        } else {
            throwResourceAlreadyExists(kafkaTopic);
        }
    }

    protected void deleteHelixResource(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        admin.dropResource(clusterName, kafkaTopic);
        logger.info("Successfully dropped the resource " + kafkaTopic + " for cluster " + clusterName);
    }

    @Override
    public SchemaEntry getKeySchema(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getKeySchema(storeName);
    }

    @Override
    public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemas(storeName);
    }

    @Override
    public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
    }

    @Override
    public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchema(storeName, id);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr) {
        checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.addValueSchema(storeName, valueSchemaStr);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId) {
        checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.addValueSchema(storeName, valueSchemaStr, schemaId);
    }

  /**
   * This function will check whether the provided schema is good to add to the provided store.
   * If yes, it will return the value schema id to be used.
   * @param clusterName
   * @param storeName
   * @param valueSchemaStr
   * @return
   */
    protected int checkPreConditionForAddValueSchemaAndGetNewSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        if (!repository.hasStore(storeName)) {
            throw new VeniceNoStoreException(storeName);
        }
        // Check compatibility
        SchemaEntry newValueSchemaWithInvalidId = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);
        Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, storeName);
        int maxValueSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
        for (SchemaEntry entry : valueSchemas) {
            // Idempotent
            if (entry.equals(newValueSchemaWithInvalidId)) {
                return entry.getId();
            }
            if (!entry.isCompatible(newValueSchemaWithInvalidId)) {
                throw new SchemaIncompatibilityException(entry, newValueSchemaWithInvalidId);
            }
            if (entry.getId() > maxValueSchemaId) {
                maxValueSchemaId = entry.getId();
            }
        }
        if (SchemaData.INVALID_VALUE_SCHEMA_ID == maxValueSchemaId) {
            return HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
        } else {
            return maxValueSchemaId + 1;
        }
    }

    @Override
    public List<String> getStorageNodes(String clusterName){
        checkControllerMastership(clusterName);
        return admin.getInstancesInCluster(clusterName);
    }

    @Override
    public Map<String, String> getStorageNodesStatus(String clusterName) {
        checkControllerMastership(clusterName);
        List<String> instances = admin.getInstancesInCluster(clusterName);
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
        Set<String> liveInstances = routingDataRepository.getLiveInstancesMap().keySet();
        Map<String, String> instancesStatusesMap = new HashMap<>();
        for (String instance : instances) {
            if (liveInstances.contains(instance)) {
                instancesStatusesMap.put(instance, InstanceStatus.CONNECTED.toString());
            } else {
                instancesStatusesMap.put(instance, InstanceStatus.DISCONNECTED.toString());
            }
        }
        return instancesStatusesMap;
    }

    @Override
    public void removeStorageNode(String clusterName, String instanceId) {
        checkControllerMastership(clusterName);
        logger.info("Removing storage node: " + instanceId + " from cluster: " + clusterName);
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
        if (routingDataRepository.getLiveInstancesMap().containsKey(instanceId)) {
            // The given storage node is still connected to cluster.
            throw new VeniceException("Storage node: " + instanceId + " is still connected to cluster: " + clusterName
                + ", could not be removed from that cluster.");
        }

        // Remove storage node from both whitelist and helix instances list.
        removeInstanceFromWhiteList(clusterName, instanceId);
        InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceId);
        admin.dropInstance(clusterName, instanceConfig);
        logger.info("Removed storage node: " + instanceId + " from cluster: " + clusterName);
    }

    @Override
    public synchronized void stop(String clusterName) {
        // Instead of disconnecting the sub-controller for the given cluster, we should disable it for this controller,
        // then the LEADER->STANDBY and STANDBY->OFFLINE will be triggered, our handler will handle the resource collection.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        admin.enablePartition(false, controllerClusterName, controllerName, clusterName, partitionNames);
        if (null != pushJobStatusWriter) {
            IOUtils.closeQuietly(pushJobStatusWriter);
        }
    }

    @Override
    public void stopVeniceController() {
        try {
            manager.disconnect();
            topicManager.close();
            zkClient.close();
            admin.close();
        } catch (Exception e) {
            throw new VeniceException("Can not stop controller correctly.", e);
        }
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
        return getOffLinePushStatus(clusterName, kafkaTopic, Optional.empty());
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic, Optional<String> incrementalPushVersion) {
        checkControllerMastership(clusterName);
        OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        Pair<ExecutionStatus, Optional<String>> statusAndDetails =
            monitor.getOfflinePushStatusAndDetails(kafkaTopic, incrementalPushVersion);
        return new OfflinePushStatusInfo(statusAndDetails.getFirst(), statusAndDetails.getSecond());
    }

    @Override
    public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
        checkControllerMastership(clusterName);
        OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        return monitor.getOfflinePushProgress(kafkaTopic);
    }

    // Create the cluster for all of parent controllers if required.
    private void createControllerClusterIfRequired(){
        if(admin.getClusters().contains(controllerClusterName)) {
            logger.info("Cluster  " + controllerClusterName + " already exists. ");
            return;
        }

        boolean isClusterCreated = admin.addCluster(controllerClusterName, false);
        if(isClusterCreated == false) {
            logger.info("Cluster  " + controllerClusterName + " Creation returned false. ");
            return;
        }
        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
            forCluster(controllerClusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));
        admin.setConfig(configScope, helixClusterProperties);
        admin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
    }

    private void createClusterIfRequired(String clusterName) {
        if(admin.getClusters().contains(clusterName)) {
            logger.info("Cluster  " + clusterName + " already exists. ");
            return;
        }

        boolean isClusterCreated = admin.addCluster(clusterName, false);
        if(isClusterCreated == false) {
            logger.info("Cluster  " + clusterName + " Creation returned false. ");
            return;
        }

        VeniceControllerClusterConfig config = controllerStateModelFactory.getClusterConfig(clusterName);
        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
                forCluster(clusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        long delayedTime = config.getDelayToRebalanceMS();
        if (delayedTime > 0) {
            helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(),
                String.valueOf(delayedTime));
        }
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), HelixUtils.TOPOLOGY_CONSTRAINT);
        admin.setConfig(configScope, helixClusterProperties);
        logger.info(
            "Cluster  " + clusterName + "  Completed, auto join to true. Delayed rebalance time:" + delayedTime);

        admin.addStateModelDef(clusterName, VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
            VeniceStateModel.getDefinition());

        admin
            .addResource(controllerClusterName, clusterName, CONTROLLER_CLUSTER_NUMBER_OF_PARTITION, LeaderStandbySMD.name,
                IdealState.RebalanceMode.FULL_AUTO.toString(), AutoRebalanceStrategy.class.getName());
        IdealState idealState = admin.getResourceIdealState(controllerClusterName, clusterName);
        // Use crush alg to allocate controller as well.
        idealState.setMinActiveReplicas(controllerClusterReplica);
        idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
        idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
        admin.setResourceIdealState(controllerClusterName, clusterName, idealState);
        admin.rebalance(controllerClusterName, clusterName, controllerClusterReplica);
    }

    private void throwStoreAlreadyExists(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " already exists. Can not add it to cluster:" + clusterName;
        logger.error(errorMessage);
        throw new VeniceStoreAlreadyExistsException(storeName, clusterName);
    }

    private void throwStoreDoesNotExist(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
        logger.error(errorMessage);
        throw new VeniceNoStoreException(storeName, clusterName);
    }

    private void throwResourceAlreadyExists(String resourceName) {
        String errorMessage = "Resource:" + resourceName + " already exists, Can not add it to Helix.";
        logAndThrow(errorMessage);
    }

    private void throwVersionAlreadyExists(String storeName, int version) {
        String errorMessage =
            "Version" + version + " already exists in Store:" + storeName + ". Can not add it to store.";
        logAndThrow(errorMessage);
    }

    private void throwClusterNotInitialized(String clusterName) {
        throw new VeniceNoClusterException(clusterName);
    }

    private void logAndThrow(String msg){
        logger.info(msg);
        throw new VeniceException(msg);
    }

    @Override
    public String getKafkaBootstrapServers(boolean isSSL) {
        if(isSSL) {
            return kafkaSSLBootstrapServers;
        } else {
            return kafkaBootstrapServers;
        }
    }

    @Override
    public boolean isSSLEnabledForPush(String clusterName, String storeName) {
        if (isSslToKafka()) {
            Store store = getStore(clusterName, storeName);
            if (store == null) {
                throw new VeniceNoStoreException(storeName);
            }
            if (store.isHybrid()) {
                if (multiClusterConfigs.getCommonConfig().isEnableNearlinePushSSLWhitelist()
                    && (!multiClusterConfigs.getCommonConfig().getPushSSLWhitelist().contains(storeName))) {
                    // whitelist is enabled but the given store is not in that list, so ssl is not enabled for this store.
                    return false;
                }
            } else {
                if (multiClusterConfigs.getCommonConfig().isEnableOfflinePushSSLWhitelist()
                    && (!multiClusterConfigs.getCommonConfig().getPushSSLWhitelist().contains(storeName))) {
                    // whitelist is enabled but the given store is not in that list, so ssl is not enabled for this store.
                    return false;
                }
            }
            // whitelist is not enabled, or whitelist is enabled and the given store is in that list, so ssl is enabled for this store for push.
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isSslToKafka() {
        return this.multiClusterConfigs.isSslToKafka();
    }

    @Override
    public TopicManager getTopicManager() {
        return this.topicManager;
    }

    @Override
    public synchronized boolean isMasterController(String clusterName) {
        VeniceDistClusterControllerStateModel model = controllerStateModelFactory.getModel(clusterName);
        if (model == null ) {
            throwClusterNotInitialized(clusterName);
        }
        return model.getCurrentState().equals(LeaderStandbySMD.States.LEADER.toString());
    }

  /**
   * Calculate number of partition for given store by give size.
   *
   * @param clusterName
   * @param storeName
   * @param storeSize
   * @return
   */
    @Override
    public int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize) {
        checkControllerMastership(clusterName);
        VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
        return PartitionCountUtils.calculatePartitionCount(clusterName, storeName, storeSize,
            getVeniceHelixResource(clusterName).getMetadataRepository(),
            getVeniceHelixResource(clusterName).getRoutingDataRepository(), config.getPartitionSize(),
            config.getNumberOfPartition(), config.getMaxNumberOfPartition());
  }

    @Override
    public int getReplicationFactor(String clusterName, String storeName) {
        //TODO if there is special config for the given store, use that value.
        return getVeniceHelixResource(clusterName).getConfig().getReplicaFactor();
    }

    @Override
    public List<Replica> getBootstrapReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getBootstrapInstances(), kafkaTopic, partition.getId(), HelixState.BOOTSTRAP_STATE);
        }
        return replicas;
    }

    @Override
    public List<Replica> getErrorReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getErrorInstances(), kafkaTopic, partition.getId(), HelixState.ERROR_STATE);
        }
        return replicas;
    }

    @Override
    public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getErrorInstances(), kafkaTopic, partition.getId(), HelixState.ERROR_STATE);
            addInstancesToReplicaList(replicas, partition.getBootstrapInstances(), kafkaTopic, partition.getId(), HelixState.BOOTSTRAP_STATE);
            addInstancesToReplicaList(replicas, partition.getReadyToServeInstances(), kafkaTopic, partition.getId(), HelixState.ONLINE_STATE);
        }
        return replicas;
    }

    private void addInstancesToReplicaList(List<Replica> replicaList, List<Instance> instancesToAdd, String resource, int partitionId, String stateOfAddedReplicas){
        for (Instance instance : instancesToAdd){
            Replica replica = new Replica(instance, partitionId, resource);
            replica.setStatus(stateOfAddedReplicas);
            replicaList.add(replica);
        }
    }

    @Override
    public List<Replica> getReplicasOfStorageNode(String cluster, String instanceId) {
        checkControllerMastership(cluster);
        return InstanceStatusDecider.getReplicasForInstance(getVeniceHelixResource(cluster), instanceId);
    }

    @Override
    public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, boolean isFromInstanceView) {
        checkControllerMastership(clusterName);
        int minActiveReplicas = getVeniceHelixResource(clusterName).getConfig().getMinActiveReplica();
        return isInstanceRemovable(clusterName, helixNodeId, minActiveReplicas, isFromInstanceView);
    }

    @Override
    public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas, boolean isInstanceView) {
        checkControllerMastership(clusterName);
        return InstanceStatusDecider
            .isRemovable(getVeniceHelixResource(clusterName), clusterName, helixNodeId, minActiveReplicas);
    }

    @Override
    public Instance getMasterController(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        LiveInstance instance = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
        if (instance == null) {
            throw new VeniceException("Can not find a master controller in the cluster:" + clusterName);
        } else {
            String instanceId = instance.getId();
            return new Instance(instanceId, Utils.parseHostFromHelixNodeIdentifier(instanceId),
                Utils.parsePortFromHelixNodeIdentifier(instanceId));
        }
    }

    @Override
    public void addInstanceToWhitelist(String clusterName, String helixNodeId) {
        checkControllerMastership(clusterName);
        whitelistAccessor.addInstanceToWhiteList(clusterName, helixNodeId);
    }

    @Override
    public void removeInstanceFromWhiteList(String clusterName, String helixNodeId) {
        checkControllerMastership(clusterName);
        whitelistAccessor.removeInstanceFromWhiteList(clusterName, helixNodeId);
    }

    @Override
    public Set<String> getWhitelist(String clusterName) {
        checkControllerMastership(clusterName);
        return whitelistAccessor.getWhiteList(clusterName);
    }

    protected void checkPreConditionForKillOfflinePush(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
    }

    @Override
    public void killOfflinePush(String clusterName, String kafkaTopic) {
        checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
        StatusMessageChannel messageChannel = getVeniceHelixResource(clusterName).getMessageChannel();
        // As we should already have retry outside of this function call, so we do not need to retry again inside.
        int retryCount = 1;
        // Broadcast kill message to all of storage nodes assigned to given resource. Helix will help us to only send
        // message to the live instances.
        // The alternative way here is that get the storage nodes in BOOTSTRAP state of given resource, then send the
        // kill message node by node. Considering the simplicity, broadcast is a better.
        // In prospective of performance, each time helix sending a message needs to read the whole LIVE_INSTANCE and
        // EXTERNAL_VIEW from ZK, so sending message nodes by nodes would generate lots of useless read requests. Of course
        // broadcast would generate useless write requests to ZK(N-M useless messages, N=number of nodes assigned to resource,
        // M=number of nodes have completed the ingestion or have not started). But considering the number of nodes in
        // our cluster is not too big, so it's not a big deal here.
        messageChannel.sendToStorageNodes(clusterName, new KillOfflinePushMessage(kafkaTopic), kafkaTopic, retryCount);
    }

    @Override
    public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = getReplicasOfStorageNode(clusterName, instanceId);
        StorageNodeStatus status = new StorageNodeStatus();
        for (Replica replica : replicas) {
            status.addStatusForReplica(HelixUtils.getPartitionName(replica.getResource(), replica.getPartitionId()),
                replica.getStatus());
        }
        return status;
    }

    // TODO we don't use this function to check the storage node status. The isRemovable looks enough to ensure the
    // TODO upgrading would not affect the push and online reading request. Leave this function here to see do we need it in the future.
    @Override
    public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId,
        StorageNodeStatus oldStatus) {
        checkControllerMastership(clusterName);
        StorageNodeStatus currentStatus = getStorageNodesStatus(clusterName, instanceId);
        return currentStatus.isNewerOrEqual(oldStatus);
    }

    @Override
    public void setDelayedRebalanceTime(String clusterName, long delayedTime) {
        boolean enable = delayedTime > 0;
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        ClusterConfig clusterConfig = manager.getHelixDataAccessor().getProperty(keyBuilder.clusterConfig());
        clusterConfig.getRecord()
            .setLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), delayedTime);
        manager.getHelixDataAccessor().setProperty(keyBuilder.clusterConfig(), clusterConfig);
        //TODO use the helix new config API below once it's ready. Right now helix has a bug that controller would not get the update from the new config.
        /* ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
        HelixConfigScope clusterScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
        configAccessor.set(clusterScope, ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(),
            String.valueOf(disable));*/
        logger.info(enable ? "Enable"
            : "Disable" + " delayed rebalance for cluster:" + clusterName + (enable ? " with delayed time:"
                + delayedTime : ""));
    }

    @Override
    public long getDelayedRebalanceTime(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        ClusterConfig clusterConfig = manager.getHelixDataAccessor().getProperty(keyBuilder.clusterConfig());
        return clusterConfig.getRecord()
            .getLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), 0l);
    }

    public void setAdminConsumerService(String clusterName, AdminConsumerService service){
        adminConsumerServices.put(clusterName, service);
    }

    @Override
    public void skipAdminMessage(String clusterName, long offset){
        if (adminConsumerServices.containsKey(clusterName)){
            adminConsumerServices.get(clusterName).setOffsetToSkip(clusterName, offset);
        } else {
            throw new VeniceException("Cannot skip offset, must first setAdminConsumerService for cluster " + clusterName);
        }

    }

    @Override
    public long getLastSucceedExecutionId(String clusterName) {
        if (adminConsumerServices.containsKey(clusterName)) {
            return adminConsumerServices.get(clusterName).getLastSucceedExecutionId(clusterName);
        } else {
            throw new VeniceException(
                "Cannot get the last succeed execution Id, must first setAdminConsumerService for cluster "
                    + clusterName);
        }
    }

    @Override
    public synchronized void setLastException(String clusterName, Exception e) {
        lastExceptionMap.put(clusterName, e);
    }

    @Override
    public synchronized Exception getLastException(String clusterName) {
        return lastExceptionMap.get(clusterName);
    }

    @Override
    public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
        return Optional.empty();
    }

    @Override
    public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
        checkControllerMastership(clusterName);
        ZkRoutersClusterManager routersClusterManager = getVeniceHelixResource(clusterName).getRoutersClusterManager();
        return routersClusterManager.getRoutersClusterConfig();
    }

    @Override
    public void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
        Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
        Optional<Integer> expectedRouterCount) {
        ZkRoutersClusterManager routersClusterManager = getVeniceHelixResource(clusterName).getRoutersClusterManager();

        checkControllerMastership(clusterName);
        if (isThrottlingEnable.isPresent()) {
            routersClusterManager.enableThrottling(isThrottlingEnable.get());
        }
        if (isMaxCapaictyProtectionEnabled.isPresent()) {
            routersClusterManager.enableMaxCapacityProtection(isMaxCapaictyProtectionEnabled.get());
        }
        if (isQuotaRebalancedEnable.isPresent() && expectedRouterCount.isPresent()) {
            routersClusterManager.enableQuotaRebalance(isQuotaRebalancedEnable.get(), expectedRouterCount.get());
        }
    }

    @Override
    public Map<String, String> getAllStorePushStrategyForMigration() {
        throw new VeniceUnsupportedOperationException("getAllStorePushStrategyForMigration");
    }

    @Override
    public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
        throw new VeniceUnsupportedOperationException("setStorePushStrategyForMigration");
    }

    @Override
    public List<String> getClusterOfStoreInMasterController(String storeName) {
        List<String> matchingClusters = new LinkedList<>();

        for (VeniceDistClusterControllerStateModel model : controllerStateModelFactory.getAllModels()) {
            Optional<VeniceHelixResources> resources = model.getResources();
            if (resources.isPresent()) {
                if (resources.get().getMetadataRepository().hasStore(storeName)) {
                    matchingClusters.add(model.getClusterName());
                }
            }
        }

        // Most of the time there should be only one matching cluster
        // During store migration there might be two matching clusters
        if (matchingClusters.size() > 2) {
            logger.warn("More than 2 matching clusters found for store " + storeName + "! Check these clusters: "
                + matchingClusters);
        }

        return matchingClusters;
    }

    @Override
    public Pair<String, String> discoverCluster(String storeName) {
        StoreConfig config = storeConfigAccessor.getStoreConfig(storeName);
        if (config == null || Utils.isNullOrEmpty(config.getCluster())) {
            throw new VeniceException("Could not find the cluster by given store: " + storeName);
        }
        String clusterName = config.getCluster();
        String d2Service = multiClusterConfigs.getClusterToD2Map().get(clusterName);
        if (d2Service == null) {
            throw new VeniceException("Could not find d2 service by given cluster: " + clusterName);
        }
        return new Pair<>(clusterName, d2Service);
    }

    @Override
    public Map<String, String> findAllBootstrappingVersions(String clusterName) {
        checkControllerMastership(clusterName);
        Map<String, String> result = new HashMap<>();
        // Find all ongoing offline pushes at first.
        OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        monitor.getTopicsOfOngoingOfflinePushes().forEach(topic -> result.put(topic, VersionStatus.STARTED.toString()));
        // Find the versions which had been ONLINE, but some of replicas are still bootstrapping due to:
        // 1. As we use N-1 strategy, so there might be some slow replicas caused by kafka or other issues.
        // 2. Storage node was added/removed/disconnected, so replicas need to bootstrap again on the same or orther node.
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
        ReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
        ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
        for (String topic : resourceAssignment.getAssignedResources()) {
            if (result.containsKey(topic)) {
                continue;
            }
            PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(topic);
            for (Partition p : partitionAssignment.getAllPartitions()) {
                if (p.getBootstrapInstances().size() > 0) {
                    String storeName = Version.parseStoreFromKafkaTopicName(topic);
                    VersionStatus status;
                    Store store = storeRepository.getStore(storeName);
                    if (store != null) {
                        status = store.getVersionStatus(Version.parseVersionFromKafkaTopicName(topic));
                    } else {
                        status = VersionStatus.NOT_CREATED;
                    }
                    result.put(topic, status.toString());
                    // Found at least one bootstrap replica, skip to next topic.
                    break;
                }
            }
        }
        return result;
    }

    public VeniceWriterFactory getVeniceWriterFactory() {
        return veniceWriterFactory;
    }

    @Override
    public VeniceControllerConsumerFactory getVeniceConsumerFactory() {
        return veniceConsumerFactory;
    }

    protected void startMonitorOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition,
        int replicationFactor, OfflinePushStrategy strategy) {
        OfflinePushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        offlinePushMonitor.setTopicReplicator(topicReplicator);
        offlinePushMonitor.startMonitorOfflinePush(
            kafkaTopic,
            numberOfPartition,
            replicationFactor,
            strategy);
    }

    protected void stopMonitorOfflinePush(String clusterName, String topic) {
        OfflinePushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        offlinePushMonitor.stopMonitorOfflinePush(topic);
    }

    protected Store checkPreConditionForUpdateStoreMetadata(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }
        return store;
    }

    @Override
    public void close() {
        manager.disconnect();
        zkClient.close();
        IOUtils.closeQuietly(topicManager);
    }

    /**
     * Check whether this controller is master or not. If not, throw the VeniceException to skip the request to
     * this controller.
     *
     * @param clusterName
     */
    protected void checkControllerMastership(String clusterName) {
        if (!isMasterController(clusterName)) {
            throw new VeniceException("This controller:" + controllerName + " is not the master of '" + clusterName
                + "'. Can not handle the admin request.");
        }
    }

    protected VeniceHelixResources getVeniceHelixResource(String cluster){
        Optional<VeniceHelixResources> resources = controllerStateModelFactory.getModel(cluster).getResources();
        if (!resources.isPresent()) {
            throwClusterNotInitialized(cluster);
        }
        return resources.get();
    }

    public void addConfig(String clusterName,VeniceControllerConfig config){
        controllerStateModelFactory.addClusterConfig(clusterName, config);
    }

    public void addMetric(String clusterName, MetricsRepository metricsRepository){

    }

    public ZkWhitelistAccessor getWhitelistAccessor() {
        return whitelistAccessor;
    }

    public StoreGraveyard getStoreGraveyard() {
        return storeGraveyard;
    }

    public String getControllerName(){
        return controllerName;
    }

    public ZkStoreConfigAccessor getStoreConfigAccessor() {
        return storeConfigAccessor;
    }

    private interface StoreMetadataOperation {
        /**
         * define the operation that update a store. Return the store after metadata being updated so that it could
         * be updated by metadataRepository
         */
        Store update(Store store);
    }

    /**
     * This function is used to detect whether current node is the master controller of controller cluster.
     *
     * Be careful to use this function since it will talk to Zookeeper directly every time.
     *
     * @return
     */
    @Override
    public boolean isMasterControllerOfControllerCluster() {
        LiveInstance leader = manager.getHelixDataAccessor().getProperty(level1KeyBuilder.controllerLeader());
        return leader.getId().equals(this.controllerName);
    }

    public void setStoreConfigForMigration(String storeName, String srcClusterName, String destClusterName) {
        StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
        storeConfig.setMigrationSrcCluster(srcClusterName);
        storeConfig.setMigrationDestCluster(destClusterName);
        storeConfigAccessor.updateConfig(storeConfig);
    }

    /**
     * This thread will run in the background and update cluster discovery information when necessary
     */
    private void startStoreMigrationMonitor() {
        Thread thread = new Thread(() -> {
            Map<String, ControllerClient> srcControllerClients = new HashMap<>();

            while (true) {
                try {
                    Utils.sleep(10000);

                    // Get a list of clusters that this controller is responsible for
                    List<String> activeClusters = this.multiClusterConfigs.getClusters()
                        .stream()
                        .filter(cluster -> this.isMasterController(cluster))
                        .collect(Collectors.toList());

                    for (String clusterName : activeClusters) {
                        // For each cluster, get a list of stores that are migrating
                        HelixReadWriteStoreRepository storeRepo = this.getVeniceHelixResource(clusterName).getMetadataRepository();
                        List<Store> migratingStores = storeRepo.listStores()
                            .stream()
                            .filter(s -> s.isMigrating())
                            .filter(s -> this.storeConfigRepo.getStoreConfig(s.getName()).get().getMigrationSrcCluster() != null)
                            .filter(s -> this.storeConfigRepo.getStoreConfig(s.getName()).get().getMigrationDestCluster() != null)
                            .collect(Collectors.toList());

                        // For each migrating stores, check if store migration is complete.
                        // If so, update cluster discovery according to storeConfig
                        for (Store store : migratingStores) {
                            String storeName = store.getName();
                            StoreConfig storeConfig = this.storeConfigRepo.getStoreConfig(storeName).get();
                            String srcClusterName = storeConfig.getMigrationSrcCluster();
                            String destClusterName = storeConfig.getMigrationDestCluster();
                            String clusterDiscovered = storeConfig.getCluster();

                            // Both src and dest controller can do the job. Just pick one.
                            if (!clusterName.equals(destClusterName)) {
                                // Source controller will ignore
                                continue;
                            }

                            if (clusterDiscovered.equals(destClusterName)) {
                                // Migration complete already
                                continue;
                            }

                            ControllerClient srcControllerClient =
                                srcControllerClients.computeIfAbsent(srcClusterName,
                                    src_cluster_name -> new ControllerClient(src_cluster_name,
                                        this.getMasterController(src_cluster_name).getUrl(false)));

                            List<Version> srcSortedOnlineVersions = srcControllerClient.getStore(storeName)
                                .getStore()
                                .getVersions()
                                .stream()
                                .sorted(Comparator.comparingInt(Version::getNumber).reversed()) // descending order
                                .filter(version -> version.getStatus().equals(VersionStatus.ONLINE))
                                .collect(Collectors.toList());

                            if (srcSortedOnlineVersions.size() == 0) {
                                logger.warn("Original store " + storeName + " in cluster " + srcClusterName + " does not have any online versions!");
                                // In this case updating cluster discovery information won't make it worse
                                this.updateClusterDiscovery(storeName, srcClusterName, destClusterName);
                                continue;
                            }
                            int srcLatestOnlineVersionNum = srcSortedOnlineVersions.get(0).getNumber();

                            Optional<Version> destLatestOnlineVersion = this.getStore(destClusterName, storeName)
                                .getVersions()
                                .stream()
                                .filter(version -> version.getStatus().equals(VersionStatus.ONLINE)
                                    && version.getNumber() >= srcLatestOnlineVersionNum)
                                .findAny();

                            if (destLatestOnlineVersion.isPresent()) {
                                logger.info(storeName + " cloned store in " + destClusterName
                                    + " is ready. Will update cluster discovery.");
                                // Switch read traffic from new clients; existing clients still need redeploy
                                this.updateClusterDiscovery(storeName, srcClusterName, destClusterName);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Caught exception in store migration monitor", e);
                }
            }
        });

        thread.start();
    }
}
