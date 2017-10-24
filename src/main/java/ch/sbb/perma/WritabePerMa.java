/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A container for a mutable persistent map.
 * <p>
 *     Is the public PerMa API for mutable maps. Loads and stores a persisted map.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class WritabePerMa<K,V> implements PerMa<K,V> {
    private final static Logger LOG = LoggerFactory.getLogger(WritabePerMa.class);

    private final ReentrantLock persistLock = new ReentrantLock();
    private final ConcurrentMap<K,V> map;

    private MapSnapshot<K,V> lastPersisted;

    private WritabePerMa(MapSnapshot<K,V> lastPersisted) {
        this.lastPersisted = lastPersisted;
        this.map = new ConcurrentHashMap<>(lastPersisted.asImmutableMap());
    }

    public static WritabePerMa loadOrCreateStringMap(File dir, String name) throws IOException {
        return loadOrCreate(dir,
                            name,
                            KeyOrValueSerializer.STRING,
                            KeyOrValueSerializer.STRING);
    }

    public static <K,V> WritabePerMa loadOrCreate(File dir,
                                                  String name,
                                                  KeyOrValueSerializer<K> keySerializer,
                                                  KeyOrValueSerializer<V> valueSerializer) throws IOException {
        LOG.info("Loading writabe PerMa {} from directory {}", name, dir);
        return new WritabePerMa<K,V>(MapSnapshot.loadOrCreate(dir, name, keySerializer, valueSerializer));
    }

    public void persist() throws IOException {
        try {
            persistLock.lock();
            LOG.debug("Persisting map");
            this.lastPersisted = lastPersisted.writeNext(map);
            LOG.debug("Persisted map to snapshot with {} entries", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    public Map<K,V> map() {
        return map;
    }
}