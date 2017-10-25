/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * A certain state in time of the persisted map, persistent or new.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
interface MapSnapshot<K, V> {
    static <K, V> MapSnapshot<K,V> loadOrCreate(File dir,
                                           String name,
                                           KeyOrValueSerializer<K> keySerializer,
                                           KeyOrValueSerializer<V> valueSerializer) throws IOException {
        if(keySerializer == null || valueSerializer == null) {
            throw new NullPointerException("keySerializer and/or valueSerializer is null");
        }
        FileGroup files = FileGroup.list(dir, name);
        if (!files.exists()) {
            return new NewMapSnapshot<K, V>(
                    name,
                    files,
                    keySerializer,
                    valueSerializer);
        }
        return PersistedMapSnapshot.load(
                name,
                files,
                keySerializer,
                valueSerializer);
    }

    MapSnapshot<K, V> writeNext(Map<K, V> currentState) throws IOException;

    MapSnapshot<K,V> refresh() throws IOException;

    MapSnapshot<K,V> compact() throws IOException;

    ImmutableMap<K, V> asImmutableMap();
}
