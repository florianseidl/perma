/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import java.util.LinkedList;

/**
 * Serialize a mutable linked list. Preferably use an ImmutableList and ImmutableListSerializer with Perma.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class LinkedListSerializer<T> extends MutableCollectionSerializer<LinkedList<T>, T> {
    public LinkedListSerializer(KeyOrValueSerializer<T> itemSerializer) {
        super(itemSerializer);
    }

    @Override
    protected LinkedList<T> createCollection(int size) {
        return new LinkedList<T>();
    }
}
