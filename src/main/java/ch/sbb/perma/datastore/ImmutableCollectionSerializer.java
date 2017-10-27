/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.collect.ImmutableCollection;

/**
 * Serialize a immutable collection as value. Each item is serialized using the given item serializer.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
abstract class ImmutableCollectionSerializer<C extends ImmutableCollection<T>, T> implements KeyOrValueSerializer<C> {
    private KeyOrValueSerializer<T> itemSerializier;

    public ImmutableCollectionSerializer(KeyOrValueSerializer<T> itemSerializier) {
        this.itemSerializier = itemSerializier;
    }

    @Override
    public byte[] toByteArray(C collection) {
        CollectionBinaryWriter writer = new CollectionBinaryWriter();
        writer.writeLength(collection.size());
        for (T item : collection) {
            writer.writeNext(itemSerializier.toByteArray(item));
        }
        return writer.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public C fromByteArray(byte[] bytes) {
        CollectionBinaryReader reader = new CollectionBinaryReader(bytes);
        int collectionSize = reader.readLength();
        ImmutableCollection.Builder<T> builder = collectionBuilder();
        for (int i = 0; i < collectionSize; i++) {
            builder.add(itemSerializier.fromByteArray(reader.readNext()));
        }
        return (C) builder.build();
    }

    abstract ImmutableCollection.Builder<T> collectionBuilder();
}
