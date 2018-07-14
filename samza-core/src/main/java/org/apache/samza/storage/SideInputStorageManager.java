package org.apache.samza.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.processors.SideInputProcessor;
import org.apache.samza.processors.SideInputProcessorFactory;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.apache.samza.util.Util;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * A storage mananger for all side input stores. It is associated with each {@link org.apache.samza.container.TaskInstance}
 * and is responsible for handling directory management, offset tracking and local checkpointing for the side input stores.
 */
public class SideInputStorageManager {
  private static final String OFFSET_FILE = "side-input-offset";
  private static final long STORE_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1); // same as changelog store delete retention
  private static final Logger LOG = LoggerFactory.getLogger(SideInputStorageManager.class);

  private final Clock clock;
  private final SideInputProcessor sideInputProcessor;
  private final Map<String, StorageEngine> stores;
  private final String storeBaseDir;
  private final Map<String, Set<SystemStreamPartition>> storeToSSps;
  private final Map<SystemStreamPartition, String> sspsToStore;
  private final StreamMetadataCache streamMetadataCache;
  private final SystemAdmins systemAdmins;
  private final TaskName taskName;

  private final ObjectMapper checkpointSerde = new ObjectMapper();
  private final StorageManagerHelper storageManagerHelper = new StorageManagerHelper();
  private final Map<SystemStreamPartition, String> lastProcessedOffsets = new HashMap<>();

  private Map<SystemStreamPartition, String> startingOffsets;

  public SideInputStorageManager(
      TaskName taskName,
      MetricsRegistry metricsRegistry,
      StreamMetadataCache streamMetadataCache,
      String storeBaseDir,
      Map<String, StorageEngine> stores,
      Map<String, Set<SystemStreamPartition>> storeToSSPs,
      SystemAdmins systemAdmins,
      Config config,
      Clock clock) {
    this.systemAdmins = systemAdmins;
    this.stores = stores;
    this.storeToSSps = storeToSSPs;
    this.streamMetadataCache = streamMetadataCache;
    this.taskName = taskName;
    this.clock = clock;

    validateStoreProperties();

    this.storeBaseDir = storeBaseDir;
    this.sideInputProcessor = Optional.ofNullable(new TaskConfigJava(config).getSideInputProcessorFactory())
        .map(sideInputFactoryName -> Util.getObj(sideInputFactoryName, SideInputProcessorFactory.class))
        .map(factory -> factory.createInstance(config, metricsRegistry))
        .orElseGet(() -> {
          if (stores.size() > 0) {
            throw new SamzaException("Missing side input processor. Make sure "
                + TaskConfigJava.SIDE_INPUT_PROCESSOR_FACTORY + " is configured correctly.");
          }

          return null;
        });

    sspsToStore = storeToSSPs.entrySet()
        .stream()
        .map(entry ->
            entry.getValue()
                .stream()
                .collect(Collectors.toMap(Function.identity(), ssp -> entry.getKey())))
        .collect(HashMap::new, HashMap::putAll, HashMap::putAll);
  }

  /**
   * Initialize the side input storage manager.
   *   1. Validates the store properties
   *   2. Directory management (clean up/setting up) for the store directories
   *   3. Loads and intializes the starting offsets for all the stores.
   */
  public void init() {
    LOG.info("Initializing side input stores.");

    startingOffsets = getStartingOffsets();
    initializeStoreDirectories();
  }

  /**
   * Flushes the contents of the underlying store to disk and the offset information to the disk.
   * It doesn't have any transactional guarantees and it is possible the contents of the store got flushed but the
   * offsets were not checkpointed.
   * However, it does guarantee that checkpointing to disk happens only after {@link StorageEngine#flush()} completes for all stores.
   */
  public void flush() {
    LOG.info("Flushing the side input stores.");

    stores.values()
        .forEach(StorageEngine::flush);

    flushOffsets();
  }

  /**
   * Stops the storage engines for all the stores and flushes the offsets to the local disk.
   * It doesn't have any transactional guarantee like {@link #flush()} and it possible that the stores are closed
   * successfully but the offsets are not checkpointed to the disk.
   */
  public void stop() {
    LOG.info("Stopping the side input stores.");

    stores.values()
        .forEach(StorageEngine::stop);

    flushOffsets();
  }

  /**
   * Fetch the {@link KeyValueStore} associated with the input {@code storeName}.
   *
   * @param storeName store name
   *
   * @return a {@link StorageEngine} associated with {@code storeName}
   */
  public StorageEngine getStore(String storeName) {
    return stores.get(storeName);
  }

  /**
   * Fetch the starting offset of the given {@link SystemStreamPartition}.
   *
   * Note: The method doesn't respect {@link StreamConfig#CONSUMER_OFFSET_DEFAULT()} and
   * {@link StreamConfig#CONSUMER_RESET_OFFSET()} configurations and will use the locally
   * checkpointed offset if its valid or fallback to oldest offset of the stream.
   *
   * @param ssp system stream partition for which the starting offset is requested
   *
   * @return the starting offset for the incoming {@link SystemStreamPartition}
   */
  public String getStartingOffset(SystemStreamPartition ssp) {
    return startingOffsets.get(ssp);
  }

  /**
   * Fetch the last processed offset for the given {@link SystemStreamPartition}.
   *
   * @param ssp system stream partition
   *
   * @return the last processed offset for the incoming {@link SystemStreamPartition}
   */
  public String getLastProcessedOffset(SystemStreamPartition ssp) {
    return lastProcessedOffsets.get(ssp);
  }

  /**
   * Processes the incoming message envelope by fetching the associated store and updates the last processed offset.
   *
   * Note: This method doesn't guarantee any checkpointing semantics. It only updates the in-memory state of the last
   * processed offset and it is possible that the tasks during container restarts can start with offsets that are older
   * than the last processed offset.
   *
   * @param messageEnvelope incoming message envelope to be processed
   */
  public void process(IncomingMessageEnvelope messageEnvelope) {
    SystemStreamPartition ssp = messageEnvelope.getSystemStreamPartition();
    String storeName = sspsToStore.get(ssp);

    KeyValueStore keyValueStore = (KeyValueStore) stores.get(storeName);
    Collection<Entry<?, ?>> entriesToBeWritten = sideInputProcessor.process(messageEnvelope, keyValueStore);
    keyValueStore.putAll(ImmutableList.copyOf(entriesToBeWritten));

    // update the last processed offset
    lastProcessedOffsets.put(ssp, messageEnvelope.getOffset());
  }

  /**
   * Flushes the offsets of all side input stores. One offset file is maintained per store and the format of the offsets
   * are as follows...
   * <pre>
   *   Offset file for SideInputStore1
   *
   *    SideInputStore1SSP1 --> offset1
   *    SideInputStore1SSP2 --> offset2
   *
   *   Offset file for SideInputStore2
   *
   *    SideInputStore2SSP1 --> offset1
   *    SideInputStore2SSP2 --> offset2
   *    SideInputStore2SSP3 --> offset3
   * </pre>
   *
   */
  void flushOffsets() {
    storeToSSps.forEach((storeName, ssps) -> {
      Map<SystemStreamPartition, String> offsets = ssps
          .stream()
          .filter(lastProcessedOffsets::containsKey)
          .collect(Collectors.toMap(Function.identity(), lastProcessedOffsets::get));

      try {
        String checkpoint = checkpointSerde.writeValueAsString(offsets);
        File offsetFile = new File(getStoreLocation(storeName), OFFSET_FILE);
        FileUtil.writeWithChecksum(offsetFile, checkpoint);
      } catch(Exception e) {
        LOG.error("Encountered error while checkpointing to the file due to", e);
        throw new SamzaException("Failed to checkpoint for side input store " + storeName, e);
      }
    });
  }

  /**
   * Loads the store offsets from the locally checkpointed file.
   * The offsets of the store are stored as tuples from {@link SystemStreamPartition} to {@link String} offset.
   *
   * <pre>
   *   SSP1 -> "offset1",
   *   SSP2 -> "offset2"
   * </pre>
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to offset.
   */
  @SuppressWarnings("unchecked")
  Map<SystemStreamPartition, String> getStoreOffsets() {
    LOG.info("Loading initial offsets from the file for side input stores.");
    Map<SystemStreamPartition, String> fileOffsets = new HashMap<>();

    stores.keySet().forEach(storeName -> {
      LOG.debug("Reading local offsets for store {}", storeName);

      File storeLocation = getStoreLocation(storeName);
      if (!isStaleStore(storeLocation)) {
        try {
          String checkpoint = storageManagerHelper.readOffsetFile(storeLocation, OFFSET_FILE);
          Map<SystemStreamPartition, String> offsets = checkpointSerde.readValue(checkpoint, Map.class);
          fileOffsets.putAll(offsets);
        } catch (Exception e) {
          LOG.warn("Failed to load the checkpoints for store " + storeName, e);
        }
      }
    });

    return fileOffsets;
  }

  /**
   * Validates the store properties for all side input stores and throws a {@link IllegalStateException} if even a single
   * side input store is configured with changelog.
   */
  private void validateStoreProperties() {
    Set<String> invalidStores = stores.entrySet()
        .stream()
        .filter(entry -> !isValidSideInputStore(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    for (String invalidStore : invalidStores) {
      LOG.debug("Invalid store configuration. Cannot configure the side input store {} with changelog", invalidStore);
    }

    Preconditions.checkState(invalidStores.isEmpty(), "Side input stores cannot be configured with changelog");
  }

  private File getStoreLocation(String storeName) {
    return new File(storeBaseDir, (storeName + File.separator + taskName.toString()).replace(' ', '_'));
  }

  /**
   * Initializes the starting offsets for the {@link SystemStreamPartition}s belonging to all the side input stores.
   * The logic to compute the starting offset is as follows...
   *   1. Loads the store offsets & filter out the offests for stale stores
   *   2. Loads the oldest store offsets from the source; e.g. Kafka
   *   3. If locally checkpointed offset is available and is greater than the oldest available offset from source, pick it
   *      Otherwise, fallback to oldest offset in the source.
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to offset
   */
  private Map<SystemStreamPartition, String> getStartingOffsets() {
    Map<SystemStreamPartition, String> fileOffsets = getStoreOffsets();
    Map<SystemStreamPartition, String> oldestOffsets = getOldestOffsets();

    Map<SystemStreamPartition, String> startingOffsets = new HashMap<>();

    sspsToStore.keySet().forEach(ssp -> {
      String fileOffset = fileOffsets.get(ssp);
      String oldestOffset = oldestOffsets.get(ssp);

      startingOffsets.put(ssp,
          storageManagerHelper.getStartingOffset(
              ssp, systemAdmins.getSystemAdmin(ssp.getSystem()), fileOffset, oldestOffset));
    });

    return startingOffsets;
  }

  /**
   * Initializes the store directories for all the stores.
   *  1. It cleans up the directories for stores that are stale defined by {@link #isStaleStore(File)}
   *  2. It checks for existence of directories and creates them if necessary
   */
  private void initializeStoreDirectories() {
    LOG.info("Initializing the store directories.");

    stores.keySet().forEach(storeName -> {
      File storeLocation = getStoreLocation(storeName);
      if (isStaleStore(storeLocation) || !storageManagerHelper.isOffsetFileValid(storeLocation, OFFSET_FILE)) {
        LOG.info("Cleaning up the store directory for {}", storeName);
        FileUtil.rm(storeLocation);
      }

      if (!storeLocation.exists()) {
        LOG.info("Creating {} as the store directory for the side input store {}", storeLocation.toPath().toString(), storeName);
        storeLocation.mkdirs();
      }
    });
  }

  /**
   * Loads the oldest offset for the {@link SystemStreamPartition}s associated with all the stores.
   * It does multiple things to obtain the oldest offsets and the logic is as follows...
   *   1. Group the list of the SSPs for the side input storage manager based on system stream
   *   2. Fetch the system stream metadata from {@link StreamMetadataCache}
   *   3. Fetch the partition metadata for each system stream and fetch the corresponding partition metadata and populate
   *      the offset for SSPs belonging to the system stream.
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to offset.
   */
  private Map<SystemStreamPartition, String> getOldestOffsets() {
    Map<SystemStreamPartition, String> oldestOffsets = new HashMap<>();

    // Step 1
    Map<SystemStream, List<SystemStreamPartition>> systemStreamToSsp= storeToSSps.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.groupingBy(SystemStreamPartition::getSystemStream));

    // Step 2
    Map<SystemStream, SystemStreamMetadata> metadata = JavaConverters.mapAsJavaMapConverter(
        streamMetadataCache.getStreamMetadata(
            JavaConverters.asScalaSetConverter(systemStreamToSsp.keySet()).asScala().toSet(), true)).asJava();

    // Step 3
    metadata.forEach((systemStream, systemStreamMetadata) -> {
      // get the partition metadata for each system stream
      Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
          systemStreamMetadata.getSystemStreamPartitionMetadata();

      // For SSPs belonging to the system stream, use the partition metadata to get the oldest offset
      Map<SystemStreamPartition, String> offsets = systemStreamToSsp.get(systemStream)
          .stream()
          .collect(
              Collectors.toMap(Function.identity(), ssp -> partitionMetadata.get(ssp.getPartition()).getOldestOffset()));

      oldestOffsets.putAll(offsets);
    });

    return oldestOffsets;
  }

  /**
   * Validates the side input store to make sure its not configured to have changelog enabled.
   *
   * @param storageEngine storage engine
   *
   * @return true - valid side input store; false - otherwise
   */
  private boolean isValidSideInputStore(StorageEngine storageEngine) {
    StoreProperties storeProperties = storageEngine.getStoreProperties();
    return storeProperties.hasSideInputs() && !storeProperties.isLoggedStore();
  }

  /**
   * Checks if the store is stale. If the time elapsed since the last modified time of the offset file is greater than
   * {@link #STORE_DELETE_RETENTION_MS}, then the store is considered stale. For stale stores, we ignore the locally
   * checkpointed offsets and go with the oldest offset from the source.
   *
   * @param storeLocation store location
   *
   * @return true if the store is stale, false otherwise
   */
  private boolean isStaleStore(File storeLocation) {
    return storageManagerHelper.isStaleStore(storeLocation, OFFSET_FILE, STORE_DELETE_RETENTION_MS);
  }
}
