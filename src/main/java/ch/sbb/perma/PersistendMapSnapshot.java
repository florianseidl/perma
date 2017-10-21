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

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
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
    private final Directory directory;
    private final ImmutableMap<K,V> mapSnapshot;
    private final MapData<K,V> persited;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    PersistendMapSnapshot(Directory directory,
                          ImmutableMap<K, V> mapSnapshot,
                          MapData persited,
                          KeyOrValueSerializer<K> keySerializer,
                          KeyOrValueSerializer<V> valueSerializer) {
        this.directory = directory;
        this.mapSnapshot = mapSnapshot;
        this.persited = persited;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    static <K,V> MapSnapshot<K,V> loadLatest(Directory directory,
                                             KeyOrValueSerializer<K> keySerializer,
                                             KeyOrValueSerializer<V> valueSerializer) throws IOException{
        Map<K,V> collector = new HashMap<>();
        Directory.Listing latestFiles = directory.listLatest();
        MapData latestData = MapData.readAllAndCollect(latestFiles.fullFile(),
                latestFiles.deltaFiles(),
                keySerializer,
                valueSerializer,
                collector);
        return new PersistendMapSnapshot<K,V>(
                directory,
                ImmutableMap.copyOf(collector),
                latestData,
                keySerializer,
                valueSerializer);
    }

    @Override
    public MapSnapshot<K,V> writeNext(Map<K,V> current) throws IOException {
        File tempFile = directory.tempFile();
        MapSnapshot snapshot = new PersistendMapSnapshot<K,V>(
                                directory,
                                ImmutableMap.copyOf(current),
                                deltaTo(current)
                                        .writeTo(tempFile,
                                                 keySerializer,
                                                 valueSerializer),
                                keySerializer,
                                valueSerializer);
        tempFile.renameTo(directory.listLatest().nextDeltaFile());
        return snapshot;
    }

    public void update() {

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
