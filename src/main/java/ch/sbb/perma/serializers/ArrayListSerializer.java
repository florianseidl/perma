/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import java.util.ArrayList;

/**
 * Serialize a mutable array list. Preferably use an ImmutableList and ImmutableListSerializer with Perma.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class ArrayListSerializer<T> extends MutableCollectionSerializer<ArrayList<T>, T> {
    public ArrayListSerializer(KeyOrValueSerializer<T> itemSerializer) {
        super(itemSerializer);
    }

    @Override
    protected ArrayList<T> createCollection(int size) {
        return new ArrayList<>(size);
    }
}
