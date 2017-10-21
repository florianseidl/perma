/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * A certain state in time of the persisted set, persistent or new.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
interface SetSnapshot<T> {
    static <T> SetSnapshot<T> loadOrCreate(File dir,
                                           String name,
                                           KeyOrValueSerializer<T> serializer) throws IOException {
        if(serializer == null) {
            throw new NullPointerException("Serializer is null");
        }
        Directory directory = new Directory(dir, name);
        if (!directory.fileExists()) {
            return new NewSetSnapshot<T>(
                    name,
                    directory,
                    serializer);
        }
        return PersistendSetSnapshot.loadLatest(
                directory,
                serializer);
    }

    SetSnapshot<T> writeNext(Set<T> currentState) throws IOException;

    ImmutableSet<T> asImmutableSet();
}
