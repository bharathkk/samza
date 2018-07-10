package org.apache.samza.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javafx.util.Pair;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;


/**
 * A storage mananger for all side input stores. It is associated with each {@link org.apache.samza.container.TaskInstance}
 * and is responsible for handling directory management, offset tracking and local checkpointing for the side input stores.
 */
public class SideInputStorageManager {
  private static final String DEFAULT_STORE_DIR = "";
  private static final String OFFSET_FILE = "side-input-offset";
  private static final long STORE_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1); // same as changelog store delete retention
  private static final Logger LOG = LoggerFactory.getLogger(SideInputStorageManager.class);

  private final Config config;
  private final MetricsRegistry metricsRegistry;
  private final Map<String, StorageEngine> stores;
  private final String storeBaseDir;
  private final Map<String, Collection<SystemStreamPartition>> storeToSSps;
  private final StreamMetadataCache streamMetadataCache;
  private final SystemAdmins systemAdmins;
  private final TaskName taskName;

  private Map<SystemStreamPartition, String> startingOffsets;
  private Map<SystemStreamPartition, String> lastProcessedOffsets;

  public SideInputStorageManager(
      TaskName taskName,
      MetricsRegistry metricsRegistry,
      StreamMetadataCache streamMetadataCache,
      Map<String, StorageEngine> stores,
      Map<String, Collection<SystemStreamPartition>> storeToSSPs,
      SystemAdmins systemAdmins,
      Config config,
      Clock clock) {
    this.metricsRegistry = metricsRegistry;
    this.config = config;
    this.systemAdmins = systemAdmins;
    this.stores = stores;
    this.storeToSSps = storeToSSPs;
    this.streamMetadataCache = streamMetadataCache;
    this.taskName = taskName;

    this.storeBaseDir = config.get("", DEFAULT_STORE_DIR);

    lastProcessedOffsets = new HashMap<>();
    startingOffsets = new HashMap<>();
  }

  /**
   * Initialize the side input storage manager.
   *   1. Validates the store properties
   *   2. Directory management (clean up/setting up) for the store directories
   *   3. Loads and intializes the starting offsets for all the stores.
   */
  public void init() {
    LOG.info("Initializing side input stores.");

    validateStoreProperties();
    initializeStartingOffsets();
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
   * Fetch the starting offsets of the {@link Set} of input {@link SystemStreamPartition}s. It denotes the starting offsets
   * for the consumer of side input store.
   *
   * Note: The method doesn't respect {@link org.apache.samza.config.StreamConfig.CONSUMER_OFFSET_DEFAULT()} and
   * {@link org.apache.samza.config.StreamConfig.CONSUMER_RESET_OFFSET()} configurations and will use the locally
   * checkpointed offset if its valid or fallback to oldest offset of the stream.
   *
   * @param ssps a set of system stream partitions
   *
   * @return a {@link Map} of {@link SystemStreamPartition} to offset
   */
  public Map<SystemStreamPartition, String> getStartingOffsets(Set<SystemStreamPartition> ssps) {
    return ssps.stream()
        .collect(Collectors.toMap(Function.identity(), startingOffsets::get));
  }

  /**
   * Updates the last processed offset for the {@link SystemStreamPartition} with the input offset.
   * The contract for the consumer of the side input store is to update the offset after the message has been delivered
   * to {@link org.apache.samza.processors.SideInputProcessor} and has been successfully processed by {@link SideInputProcessor}.
   *
   * Note: This method doesn't guarantee any checkpointing semantics. It only updates the in-memory state of the last
   * processed offset and it is possible that the tasks during container restarts can start with offsets that are older
   * than the last processed offset.
   *
   * @param ssp system stream partition
   * @param offset offset of the message that was last processed for the input system stream partition
   */
  public void updateLastProcessedOffset(SystemStreamPartition ssp, String offset) {
    lastProcessedOffsets.put(ssp, offset);
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
          .collect(Collectors.toMap(Function.identity(), lastProcessedOffsets::get));
      File offsetFile = new File(getStoreLocation(storeName), OFFSET_FILE);
      FileUtil.writeWithChecksum(offsetFile, offsets);
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
  Map<SystemStreamPartition, String> loadStoreOffsets() {
    LOG.info("Loading initial offsets from the file for side input stores.");
    Map<SystemStreamPartition, String> fileOffsets = new HashMap<>();

    stores.keySet().forEach(storeName -> {
      LOG.debug("Reading local offsets for store {}", storeName);

      File offsetFile = new File(getStoreLocation(storeName), OFFSET_FILE);
      fileOffsets.putAll((HashMap<SystemStreamPartition, String>) FileUtil.readWithChecksum(offsetFile));
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
        .filter(entry -> isValidSideInputStore(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    for (String invalidStore : invalidStores) {
      LOG.debug("Invalid store configuration. Cannot configure the side input store {} with changelog", invalidStore);
    }

    Preconditions.checkState(invalidStores.isEmpty() == true, "Side input stores cannot be configured with changelog");
  }

  private File getStoreLocation(String storeName) {
    return new File(storeBaseDir, (taskName.toString() + File.separator + storeName).replace(' ', '_'));
  }

  /**
   * Initializes the starting offsets for the {@link SystemStreamPartition}s belonging to all the side input stores.
   * The logic to compute the starting offset is as follows...
   *   1. Loads the store offsets & filter out the offests for stale stores
   *   2. Loads the oldest store offsets from the source; e.g. Kafka
   *   3. If locally checkpointed offset is available and is greater than the oldest available offset from source, pick it
   *      Otherwise, fallback to oldest offset in the source.
   */
  private void initializeStartingOffsets() {
    Map<SystemStreamPartition, String> fileOffsets = loadStoreOffsets();
    Map<SystemStreamPartition, String> sanitizedFileOffsets = sanitizeFileOffsets(fileOffsets);
    Map<SystemStreamPartition, String> oldestOffsets = loadOldestOffsets();

    oldestOffsets.forEach((ssp, oldestOffset) -> {
      String fileOffset = sanitizedFileOffsets.get(ssp);
      String startingOffset;

      if (fileOffset != null && admin.offsetComparator(fileOffset, oldestOffset) > 0) {
        // can be optimized to make calls for all SSPs per system stream at once
        startingOffset = admin.getOffsetsAfter(ImmutableMap.of(ssp, fileOffset).get(ssp));
      } else {
        startingOffset = oldestOffset;
      }

      startingOffsets.put(ssp, startingOffset);
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
  private Map<SystemStreamPartition, String> loadOldestOffsets() {
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
   * @param storeName store name
   *
   * @return true if the store is stale, false otherwise
   */
  private boolean isStaleStore(String storeName) {
    File storeLocation = getStoreLocation(storeName);
    boolean isStaleStore = false;

    if (storeLocation.exists()) {
      File offsetFile = new File(storeLocation, storeName);
      long offsetFileLastModifiedTime = offsetFile.lastModified();
      isStaleStore = (clock.currentTimeMillis() - offsetFileLastModifiedTime) >= STORE_DELETE_RETENTION_MS;

      if (isStaleStore) {
        LOG.info(String.format("Store: %s is stale since lastModifiedTime of offset file: %s, is older than "
            + "changelog deleteRetentionMs: %s.", storeName, offsetFileLastModifiedTime, STORE_DELETE_RETENTION_MS));
      }
    } else {
      LOG.info("Logged storage partition directory: " + storeLocation + " does not exist.")
    }

    return isStaleStore;
  }
}
