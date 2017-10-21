/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A container for a mutable persistent set.
 * <p>
 * Is the public API to PerMa for Sets and knows how to load and store a persisted map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class WritabePerMaSet<T> implements PerMaSet<T> {
    private final ReentrantLock persistLock = new ReentrantLock();
    private final Set<T> set;

    private SetSnapshot<T> lastPersisted;

    private WritabePerMaSet(SetSnapshot<T> lastPersisted) {
        this.lastPersisted = lastPersisted;
        this.set = toMutableSet(lastPersisted.asImmutableSet());
    }

    private Set<T> toMutableSet(ImmutableSet<T> snapshot) {
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
        return new WritabePerMaSet<T>(SetSnapshot.loadOrCreate(dir, name, serializer));
    }

    public void persist() throws IOException {
        try {
            persistLock.lock();
            this.lastPersisted = lastPersisted.writeNext(set);
        }
        finally {
            persistLock.unlock();
        }
    }

    @Override
    public Set<T> set() {
        return set;
    }
}