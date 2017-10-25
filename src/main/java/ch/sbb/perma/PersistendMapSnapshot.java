/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import ch.sbb.perma.datastore.MapData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A snapshot of a map that is persisted to PerMa files.
 * <p>
 * Knows how to load and store a complete snapshot of the map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class PersistendMapSnapshot<K,V> implements MapSnapshot<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(MapSnapshot.class);

    private final FileGroup files;
    private final ImmutableMap<K,V> mapSnapshot;
    private final MapData<K,V> persited;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    PersistendMapSnapshot(FileGroup directorySnapshot,
                          ImmutableMap<K, V> mapSnapshot,
                          MapData persited,
                          KeyOrValueSerializer<K> keySerializer,
                          KeyOrValueSerializer<V> valueSerializer) {
        this.files = directorySnapshot;
        this.mapSnapshot = mapSnapshot;
        this.persited = persited;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    static <K,V> MapSnapshot<K,V> load(FileGroup latestFiles,
                                       KeyOrValueSerializer<K> keySerializer,
                                       KeyOrValueSerializer<V> valueSerializer) throws IOException{
        LOG.debug("Loading persisted Snapshot from files latestFiles");
        Map<K,V> collector = new HashMap<>();
        MapData latestData = MapData.readFileGroupAndCollect(
                latestFiles.fullFile(),
                latestFiles.deltaFiles(),
                keySerializer,
                valueSerializer,
                collector);
        return new PersistendMapSnapshot<K,V>(
                latestFiles,
                ImmutableMap.copyOf(collector),
                latestData,
                keySerializer,
                valueSerializer);
    }

    @Override
    public MapSnapshot<K,V> writeNext(Map<K,V> current) throws IOException {
        return writeNext(ImmutableMap.copyOf(current));
    }

    public MapSnapshot<K,V> writeNext(ImmutableMap<K,V> currentImmutable) throws IOException {
        FileGroup filesWithNextDeltaFile = files.withNextDelta();
        MapData<K,V> delta = deltaTo(currentImmutable);
        if(delta.isEmpty()) {
            LOG.debug("Noting to write (no changes detected), ignoring");
            return this;
        }
        MapData<K,V> nextDeltaData = delta.writeTo(
                                                filesWithNextDeltaFile.latestDeltaFile(),
                                                keySerializer,
                                                valueSerializer);
        LOG.debug("Writing delta file with size={} to file {}",
                currentImmutable.size(),
                filesWithNextDeltaFile.latestDeltaFile());

        return new PersistendMapSnapshot<K,V>(
                                filesWithNextDeltaFile,
                                currentImmutable,
                                nextDeltaData,
                                keySerializer,
                                valueSerializer);
    }

    @Override
    public MapSnapshot<K, V> refresh() throws IOException {
        FileGroup refresheFiles = files.refresh();
        List<File> additionalDeltaFiles = refresheFiles.deltaFilesSince(files);
        if(additionalDeltaFiles.isEmpty()) {
            LOG.debug("No new files found, cancelling refresh");
            return this;
        }
        LOG.debug("Refreshing from files {}", additionalDeltaFiles);
        Map<K,V> collector = new HashMap<>(mapSnapshot);
        MapData<K,V> lastData = persited.updateWithDeltasAndCollect(
                                additionalDeltaFiles,
                                keySerializer,
                                valueSerializer,
                                collector);
        return new PersistendMapSnapshot<K,V>(
                                refresheFiles,
                                ImmutableMap.copyOf(collector),
                                lastData,
                                keySerializer,
                                valueSerializer);
    }

    @Override
    public ImmutableMap<K,V> asImmutableMap() {
        return mapSnapshot;
    }

    private MapData<K,V> deltaTo(Map<K,V> current) {
        MapDifference<K,V> diff = Maps.difference(mapSnapshot, current);
        Set<K> deleted = diff.entriesOnlyOnLeft().keySet();
        Map<K,V> newEntries = diff.entriesOnlyOnRight();
        Map<K,V> modifiedEntries =
                diff.entriesDiffering().entrySet().stream()
                        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().rightValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        modifiedEntries.putAll(newEntries);
        return persited.nextDelta(ImmutableMap.copyOf(modifiedEntries), ImmutableSet.copyOf(deleted));
    }

}
