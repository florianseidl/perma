/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ForwardingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Read only persistent map.
 * <p>
 *     Is the public Writable API for immutable maps. Loads a persisted map and updates it on request.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ReadOnlyPerma<K,V> extends ForwardingMap<K,V> implements RefreshableMap<K,V> {
    private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyPerma.class);

    private MapSnapshot<K,V> lastLoaded;
    private final ReentrantLock loadLock = new ReentrantLock();

    private ReadOnlyPerma(MapSnapshot<K, V> loaded) {
        this.lastLoaded = loaded;
    }

    public static ReadOnlyPerma loadStringMap(File dir, String name) throws IOException {
        return load(dir,
                    name,
                    KeyOrValueSerializer.STRING,
                    KeyOrValueSerializer.STRING);
    }

    public static <K,V> ReadOnlyPerma load(File dir,
                                           String name,
                                           KeyOrValueSerializer<K> keySerializer,
                                           KeyOrValueSerializer<V> valueSerializer) throws IOException {
        LOG.info("Loading readonly Perma {} from directory {}", name, dir);
        return new ReadOnlyPerma<>(MapSnapshot.loadOrCreate(dir, name, keySerializer, valueSerializer));
    }

    public void refresh() throws IOException {
        try {
            loadLock.lock();
            LOG.debug("Refreshing map");
            lastLoaded = lastLoaded.refresh();
            LOG.info("Refreshing map to snapshot with {} entries", lastLoaded.asImmutableMap().size());
        }
        finally {
            loadLock.unlock();
        }
    }

    @Override
    protected Map<K, V> delegate() {
        return lastLoaded.asImmutableMap();
    }
}
