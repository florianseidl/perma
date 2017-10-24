/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static ch.sbb.perma.SetValueSerializer.TO_NULL;

/**
 * Read only persistent set.
 * <p>
 *     Is the public PerMa API for immutable set. Loads a persisted set and updates it on request.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ReadOnlyPerMaSet<T> implements PerMaSet<T> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyPerMaSet.class);

    private MapSnapshot<T,Object> lastLoaded;
    private final ReentrantLock loadLock = new ReentrantLock();

    private ReadOnlyPerMaSet(MapSnapshot<T,Object> loaded) {
        this.lastLoaded = loaded;
    }

    public static ReadOnlyPerMaSet<String> loadStringSet(File dir, String name) throws IOException {
        return load(dir,
                    name,
                    KeyOrValueSerializer.STRING);
    }

    public static <T> ReadOnlyPerMaSet<T> load(File dir,
                                              String name,
                                              KeyOrValueSerializer<T> serializer) throws IOException {
        LOG.info("Loading readonly PerMaSet {} from directory {}", name, dir);
        return new ReadOnlyPerMaSet<T>(MapSnapshot.loadOrCreate(dir, name, serializer, TO_NULL));
    }

    public void udpate() throws IOException {
        try {
            loadLock.lock();
            LOG.debug("Updating map");
            lastLoaded = lastLoaded.refresh();
            LOG.debug("Loaded set snapshot with {} entries", lastLoaded.asImmutableMap().size());
        }
        finally {
            loadLock.unlock();
        }
    }

    @Override
    public Set<T> set() {
        return lastLoaded.asImmutableMap().keySet();
    }
}
