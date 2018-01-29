/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import java.util.HashSet;

/**
 * Serialize a mutable hash set. Preferably use an ImmutableSet and ImmutableSetSerializer with Perma.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class HashSetSerializer<T> extends MutableCollectionSerializer<HashSet<T>, T> {

    public HashSetSerializer(KeyOrValueSerializer<T> itemSerializer) {
        super(itemSerializer);
    }

    @Override
    protected HashSet<T> createCollection(int size) {
        return new HashSet<>(size);
    }
}
