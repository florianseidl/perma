/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import ch.sbb.perma.datastore.MapData;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(MapSnapshot.class);

    private final String name;
    private final FileGroup files;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    NewMapSnapshot(String name, FileGroup directory, KeyOrValueSerializer<K> keySerializer, KeyOrValueSerializer<V> valueSerializer) {
        LOG.debug("Creating new Snapshot");
        this.name = name;
        this.files = directory;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public MapSnapshot writeNext(Map<K,V> current)  throws IOException {
        return writeNext(ImmutableMap.copyOf(current));
    }

    public MapSnapshot writeNext(ImmutableMap<K,V> currentImmutable)  throws IOException{
        if(currentImmutable.isEmpty()) {
            LOG.debug("Noting to write (map is still empty), ignoring");
            return this;
        }
        FileGroup newFullFileGroup = files.withNextFull();
        MapData fullData = MapData
                                .createNewFull(name, currentImmutable)
                                .writeTo(newFullFileGroup.fullFile(),
                                         keySerializer,
                                         valueSerializer);
        LOG.debug("Writing full file with mapSize={} to file {}",
                    currentImmutable.size(),
                    newFullFileGroup.fullFile());
        return new PersistendMapSnapshot<>(
                newFullFileGroup,
                currentImmutable,
                fullData,
                keySerializer,
                valueSerializer);
    }

    @Override
    public MapSnapshot<K, V> refresh() throws IOException {
        FileGroup refreshedFiles = files.refresh();
        if(!refreshedFiles.exists()) {
            LOG.debug("No file found, cancelling refresh");
            return this;
        }
        return PersistendMapSnapshot.load(refreshedFiles, keySerializer, valueSerializer);
    }

    @Override
    public ImmutableMap<K,V> asImmutableMap() {
        return ImmutableMap.of();
    }
}
