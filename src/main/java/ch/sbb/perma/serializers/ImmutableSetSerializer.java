/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;

/**
 * Serialize an immutable set. Each item is serialized using the given item serializer.
 * <p>
 *     Use this serializier to build maps of sets.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ImmutableSetSerializer<T> extends ImmutableCollectionSerializer<ImmutableSet<T>,T>{
    public ImmutableSetSerializer(KeyOrValueSerializer<T> itemSerializier) {
        super(itemSerializier);
    }

    @Override
    ImmutableCollection.Builder<T> collectionBuilder() {
        return ImmutableSet.builder();
    }
}
