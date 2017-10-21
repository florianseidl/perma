/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import ch.sbb.perma.datastore.MapData;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static ch.sbb.perma.SetValueSerializer.NULL_OBJECT;
import static ch.sbb.perma.SetValueSerializer.TO_NULL;

/**
 * A snapshot of a set that is persisted to PerMa files.
 * <p>
 * Knows how to load and store a complete snapshot of the set.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class PersistendSetSnapshot<T> implements SetSnapshot<T> {
    private final Directory directory;
    private final ImmutableSet<T> setSnapshot;
    private final MapData<T, Object> persited;
    private final KeyOrValueSerializer<T> serializer;

    PersistendSetSnapshot(Directory directory,
                          ImmutableSet<T> setSnapshot,
                          MapData persited,
                          KeyOrValueSerializer<T> serializer) {
        this.directory = directory;
        this.setSnapshot = setSnapshot;
        this.persited = persited;
        this.serializer = serializer;
    }

    @Override
    public SetSnapshot<T> writeNext(Set<T> current) throws IOException {
        File tempFile = directory.tempFile();
        SetSnapshot snapshot = new PersistendSetSnapshot<T>(
                                directory,
                                ImmutableSet.copyOf(current),
                                deltaTo(current)
                                        .writeTo(tempFile,
                                                 serializer,
                                                 TO_NULL),
                                serializer);
        tempFile.renameTo(directory.listLatest().nextDeltaFile());
        return snapshot;
    }

    static <T>  SetSnapshot<T> loadLatest(Directory directory,
                                           KeyOrValueSerializer<T> serializer) throws IOException{
        Map<T,Object> collector = new HashMap<>();
        Directory.Listing latestFiles = directory.listLatest();
        MapData latestData = MapData.readAllAndCollect(latestFiles.fullFile(),
                                                        latestFiles.deltaFiles(),
                                                        serializer,
                                                        TO_NULL,
                                                        collector);
        return new PersistendSetSnapshot<T>(
                directory,
                ImmutableSet.copyOf(collector.keySet()),
                latestData,
                serializer);
    }

    @Override
    public ImmutableSet<T> asImmutableSet() {
        return setSnapshot;
    }

    private MapData<T,Object> deltaTo(Set<T> current) {
        Sets.SetView<T> deleted = Sets.difference(setSnapshot, current);
        Sets.SetView<T> added = Sets.difference(current, setSnapshot);
        return persited.nextDelta(Maps.toMap(added, key -> NULL_OBJECT), ImmutableSet.copyOf(deleted));
    }

}
