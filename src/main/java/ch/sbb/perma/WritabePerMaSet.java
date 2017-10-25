/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static ch.sbb.perma.datastore.NullValueSerializer.NULL;
import static ch.sbb.perma.datastore.NullValueSerializer.NULL_OBJECT;

/**
 * A container for a mutable persistent set.
 * <p>
 *   Is the public PerMa API for mutable sets. Load and store a persisted sets.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class WritabePerMaSet<T> implements PerMaSet<T> {
    private final static Logger LOG = LoggerFactory.getLogger(WritabePerMaSet.class);

    private final ReentrantLock persistLock = new ReentrantLock();
    private final Set<T> set;

    private MapSnapshot<T, Object> lastPersisted;

    private WritabePerMaSet(MapSnapshot<T, Object> lastPersisted) {
        this.lastPersisted = lastPersisted;
        this.set = toMutableSet(lastPersisted.asImmutableMap().keySet());
    }

    private Set<T> toMutableSet(Set<T> snapshot) {
        ConcurrentHashMap.KeySetView<T, Boolean> set = ConcurrentHashMap.newKeySet(snapshot.size());
        set.addAll(snapshot);
        return set;
    }

    public static WritabePerMaSet loadOrCreateStringSet(File dir, String name) throws IOException {
        return loadOrCreate(dir,
                            name,
                            KeyOrValueSerializer.STRING);
    }

    public static <T> WritabePerMaSet loadOrCreate(File dir,
                                                     String name,
                                                     KeyOrValueSerializer<T> serializer) throws IOException {
        LOG.info("Loading writabe PerMaSet {} from directory {}", name, dir);
        return new WritabePerMaSet<T>(MapSnapshot.loadOrCreate(dir, name, serializer, NULL));
    }

    public void persist() throws IOException {
        try {
            persistLock.lock();
            LOG.debug("Persisting set");
            this.lastPersisted = lastPersisted.writeNext(setAsMap(set));
            LOG.info("Persisted set to snapshot with {} entries", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    public void compact() throws IOException {
        try {
            persistLock.lock();
            persist();
            LOG.debug("Compacting set");
            this.lastPersisted = lastPersisted.compact();
            LOG.info("Compacted set to snapshot with {} entries", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    @Override
    public Set<T> set() {
        return set;
    }

    private static <T> ImmutableMap<T, Object> setAsMap(Set<T> set) {
        return Maps.toMap(set, key -> NULL_OBJECT);
    }
}