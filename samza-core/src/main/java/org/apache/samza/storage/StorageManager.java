package org.apache.samza.storage;

import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamPartition;


public abstract class StorageManager {
  private final MetricsRegistry metricsRegistry;
  private final Config config;
  private final Map<String, StorageEngine> stores;
  private final String storeDir;
  private final String defaultStoreDir;
  private final SystemAdmins systemAdmins;

  private Map<SystemStreamPartition, String> fileOffsets;
  private Map<SystemStreamPartition, String> oldestOffsets;

  StorageManager(
      MetricsRegistry metricsRegistry,
      Config config,
      Map<String, StorageEngine> stores,
      SystemAdmins systemAdmins,
      String storeDir,
      String defaultStoreDir) {
    this.metricsRegistry = metricsRegistry;
    this.config = config;
    this.defaultStoreDir = defaultStoreDir;
    this.systemAdmins = systemAdmins;
    this.storeDir = storeDir;
    this.stores = stores;
  }

  public void init() {

    fileOffsets = getFileOffsets();
    oldestOffsets = getOldestOffsets()
  }

  public KeyValueStore getStore(String storeName) {
    return (KeyValueStore) stores.get(storeName);
  }

  /**
   *
   * @return
   */
  public abstract Map<SystemStreamPartition, String> getFileOffsets();

  /**
   *
   * @param ssps
   * @return
   */
  public abstract Map<SystemStreamPartition, String> getOldestOffsets(Set<SystemStreamPartition> ssps);

  protected String getStartingOffset(SystemStreamPartition ssp) {

  }
}
