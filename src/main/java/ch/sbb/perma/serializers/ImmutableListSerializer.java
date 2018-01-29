/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

/**
 * Serialize an immutable list. Each item is serialized using the given item serializer.
 * <p>
 *     Use this serializer to build maps of lists.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ImmutableListSerializer<T> extends ImmutableCollectionSerializer<ImmutableList<T>,T>{
    public ImmutableListSerializer(KeyOrValueSerializer<T> itemSerializier) {
        super(itemSerializier);
    }

    @Override
    protected ImmutableCollection.Builder<T> collectionBuilder() {
        return ImmutableList.builder();
    }
}
