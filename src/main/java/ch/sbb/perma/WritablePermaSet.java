/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static ch.sbb.perma.serializers.NullValueSerializer.NULL;
import static ch.sbb.perma.serializers.NullValueSerializer.NULL_OBJECT;

/**
 * A mutable persistent set.
 * <p>
 *   Is the public Writable API for mutable sets. Load and store a persisted sets.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class WritablePermaSet<T> extends ForwardingSet<T> implements WritableSet<T> {
    private final static Logger LOG = LoggerFactory.getLogger(WritablePermaSet.class);

    private final ReentrantLock persistLock = new ReentrantLock();
    private final Set<T> set;

    private MapSnapshot<T, Object> lastPersisted;

    private WritablePermaSet(MapSnapshot<T, Object> lastPersisted) {
        this.lastPersisted = lastPersisted;
        this.set = toMutableSet(lastPersisted.asImmutableMap().keySet());
    }

    private Set<T> toMutableSet(Set<T> snapshot) {
        ConcurrentHashMap.KeySetView<T, Boolean> set = ConcurrentHashMap.newKeySet(snapshot.size());
        set.addAll(snapshot);
        return set;
    }

    public static WritablePermaSet<String> loadOrCreateStringSet(File dir, String name) throws IOException {
        return loadOrCreateStringSet(dir, name, Options.defaults());
    }

    public static WritablePermaSet<String> loadOrCreateStringSet(File dir, String name, Options options) throws IOException {
        return WritablePermaSet.loadOrCreate(dir,
                                            name,
                                            KeyOrValueSerializer.STRING,
                                            options);
    }

    public static <T> WritablePermaSet<T> loadOrCreate(File dir,
                                                       String name,
                                                       KeyOrValueSerializer<T> serializer) throws IOException {
        return loadOrCreate(dir, name, serializer, Options.defaults());
    }

    public static <T> WritablePermaSet<T> loadOrCreate(File dir,
                                                       String name,
                                                       KeyOrValueSerializer<T> serializer,
                                                       Options options) throws IOException {
        LOG.info("Loading writabe PermaSet {} from directory {} with options {}", name, dir, options);
        return new WritablePermaSet<>(MapSnapshot.loadOrCreate(dir, name, options, serializer, NULL));
    }

    public void persist() throws IOException {
        try {
            persistLock.lock();
            LOG.debug("Persisting set");
            this.lastPersisted = lastPersisted.writeNext(setAsMap(set));
            LOG.info("Persisted set with {} entries to snapshot", lastPersisted.asImmutableMap().size());
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
            LOG.info("Compacted set with {} entries", lastPersisted.asImmutableMap().size());
        }
        finally {
            persistLock.unlock();
        }
    }

    private static <T> ImmutableMap<T, Object> setAsMap(Set<T> set) {
        return Maps.toMap(set, key -> NULL_OBJECT);
    }

    @Override
    protected Set<T> delegate() {
        return set;
    }
}