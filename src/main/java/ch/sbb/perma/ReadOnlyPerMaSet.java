/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import com.google.common.collect.ForwardingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static ch.sbb.perma.datastore.NullValueSerializer.NULL;

/**
 * Read only persistent set.
 * <p>
 *     Is the public Writeable API for immutable set. Loads a persisted set and updates it on request.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ReadOnlyPerMaSet<T> extends ForwardingSet<T> implements Refreshable {
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
        return new ReadOnlyPerMaSet<T>(MapSnapshot.loadOrCreate(dir, name, serializer, NULL));
    }

    public void refresh() throws IOException {
        try {
            loadLock.lock();
            LOG.debug("Refreshing set");
            lastLoaded = lastLoaded.refresh();
            LOG.info("Refreshed set snapshot with {} entries", lastLoaded.asImmutableMap().size());
        }
        finally {
            loadLock.unlock();
        }
    }

    @Override
    protected Set<T> delegate() {
        return lastLoaded.asImmutableMap().keySet();
    }
}
