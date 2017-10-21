/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ReadOnlyPerMa<K,V> implements PerMa<K,V> {
    private MapSnapshot<K,V> lastLoaded;
    private final ReentrantLock loadLock = new ReentrantLock();

    public ReadOnlyPerMa(MapSnapshot<K, V> loaded) {
        this.lastLoaded = loaded;
    }

    public static ReadOnlyPerMa loadStringMap(File dir, String name) throws IOException {
        return load(dir,
                    name,
                    KeyOrValueSerializer.STRING,
                    KeyOrValueSerializer.STRING);
    }

    public static <K,V> ReadOnlyPerMa load(File dir,
                                              String name,
                                              KeyOrValueSerializer<K> keySerializer,
                                              KeyOrValueSerializer<V> valueSerializer) throws IOException {
        return new ReadOnlyPerMa<K,V>(MapSnapshot.loadOrCreate(dir, name, keySerializer, valueSerializer));
    }

    public void udpate() {

    }


    @Override
    public Map<K, V> map() {
        return null;
    }
}
