/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ForwardingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static ch.sbb.perma.serializers.NullValueSerializer.NULL;

/**
 * Read only persistent set.
 * <p>
 *     Is the public Writable API for immutable set. Loads a persisted set and updates it on request.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ReadOnlyPermaSet<T> extends ForwardingSet<T> implements RefreshableSet<T> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyPermaSet.class);

    private MapSnapshot<T,Object> lastLoaded;
    private final ReentrantLock loadLock = new ReentrantLock();

    private ReadOnlyPermaSet(MapSnapshot<T,Object> loaded) {
        this.lastLoaded = loaded;
    }

    public static ReadOnlyPermaSet<String> loadStringSet(File dir, String name) throws IOException {
        return load(dir,
                    name,
                    KeyOrValueSerializer.STRING);
    }

    public static <T> ReadOnlyPermaSet<T> load(File dir,
                                               String name,
                                               KeyOrValueSerializer<T> serializer) throws IOException {
        LOG.info("Loading readonly PermaSet {} from directory {}", name, dir);
        return new ReadOnlyPermaSet<T>(MapSnapshot.loadOrCreate(dir, name, Options.illegal(), serializer, NULL));
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
