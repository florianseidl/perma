/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import ch.sbb.perma.datastore.MapData;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * A snapshot of a map that was never persisted.
 * <p>
 * Knows how create a new persisted snapshot of the map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class NewMapSnapshot<K,V> implements MapSnapshot<K,V> {
    private final String name;
    private final Directory directory;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    public NewMapSnapshot(String name, Directory directory, KeyOrValueSerializer<K> keySerializer, KeyOrValueSerializer<V> valueSerializer) {
        this.name = name;
        this.directory = directory;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public MapSnapshot writeNext(Map<K,V> current)  throws IOException{
        ImmutableMap<K,V> currentImmutable = ImmutableMap.copyOf(current);
        File tempFile = directory.tempFile();
        MapData fullData = MapData
                                .createNewFull(name, currentImmutable)
                                .writeTo(tempFile,
                                         keySerializer,
                                         valueSerializer);
        tempFile.renameTo(directory.nextFullFile());
        return new PersistendMapSnapshot<K,V>(
                directory,
                currentImmutable,
                fullData,
                keySerializer,
                valueSerializer);
    }

    @Override
    public ImmutableMap<K,V> asImmutableMap() {
        return ImmutableMap.of();
    }
}
