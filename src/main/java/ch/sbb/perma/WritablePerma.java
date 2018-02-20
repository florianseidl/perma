/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ForwardingConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A mutable persistent map.
 * <p>
 *     Is the public Writable API for mutable maps. Loads and stores a persisted map.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class WritablePerma<K,V> extends ForwardingConcurrentMap<K,V> implements WritableMap<K,V> {
    private final static Logger LOG = LoggerFactory.getLogger(WritablePerma.class);

    private final ReentrantLock persistLock = new ReentrantLock();
    private final ConcurrentMap<K,V> map;

    private MapSnapshot<K,V> lastPersisted;

    private WritablePerma(MapSnapshot<K,V> lastPersisted) {
        this.lastPersisted = lastPersisted;
        this.map = new ConcurrentHashMap<>(lastPersisted.asImmutableMap());
    }

    public static WritablePerma<String, String> loadOrCreateStringMap(File dir, String name) throws IOException {
        return loadOrCreate(dir,
                            name,
                            KeyOrValueSerializer.STRING,
                            KeyOrValueSerializer.STRING);
    }

    public static <K,V> WritablePerma<K,V> loadOrCreate(File dir,
                                                        String name,
                                                        KeyOrValueSerializer<K> keySerializer,
                                                        KeyOrValueSerializer<V> valueSerializer) throws IOException {
        LOG.info("Loading writabe Perma {} from directory {}", name, dir);
        return new WritablePerma<>(MapSnapshot.loadOrCreate(dir, name, keySerializer, valueSerializer));
    }

    public void persist() throws IOException {
        try {
            persistLock.lock();
            LOG.debug("Persisting map");
            this.lastPersisted = lastPersisted.writeNext(map);
            LOG.info("Persisted map with {} entries to snapshot", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    public void compact() throws IOException {
        try {
            persistLock.lock();
            persist();
            LOG.debug("Compacting map");
            this.lastPersisted = lastPersisted.compact();
            LOG.info("Compacted map with {} entries", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    @Override
    protected ConcurrentMap<K, V> delegate() {
        return map;
    }
}