package org.apache.samza.processors;

import java.util.Collection;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;


/**
 *
 */
public interface SideInputProcessor {

  /**
   *
   * @param ime
   * @param store
   * @param <K>
   * @param <V>
   *
   * @return
   */
  <K,V> Collection<Entry<K, V>> process(IncomingMessageEnvelope ime, KeyValueStore<K, V> store);
}
