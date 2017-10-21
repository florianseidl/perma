/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import ch.sbb.perma.datastore.MapData;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import static ch.sbb.perma.SetValueSerializer.NULL_OBJECT;
import static ch.sbb.perma.SetValueSerializer.TO_NULL;

/**
 * A snapshot of a set that was never persisted.
 * <p>
 * Knows how create a new persisted snapshot of the set.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class NewSetSnapshot<T> implements SetSnapshot<T> {
    private final String name;
    private final Directory directory;
    private final KeyOrValueSerializer<T> serializer;

    public NewSetSnapshot(String name, Directory directory, KeyOrValueSerializer<T> serializer) {
        this.name = name;
        this.directory = directory;
        this.serializer = serializer;
    }

    @Override
    public SetSnapshot<T> writeNext(Set<T> current)  throws IOException{
        ImmutableSet<T> currentImmutable = ImmutableSet.copyOf(current);
        File tempFile = directory.tempFile();
        MapData<T, Object> fullData = MapData
                                .createNewFull(name, Maps.toMap(currentImmutable, key -> NULL_OBJECT))
                                .writeTo(tempFile,
                                         serializer,
                                         TO_NULL);
        tempFile.renameTo(directory.nextFullFile());
        return new PersistendSetSnapshot<T>(
                directory,
                currentImmutable,
                fullData,
                serializer);
    }

    @Override
    public ImmutableSet<T> asImmutableSet() {
        return ImmutableSet.of();
    }
}
