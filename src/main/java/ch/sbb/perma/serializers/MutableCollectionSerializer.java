/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import java.util.Collection;

/**
 * Serialize a mutable collection. Preferably use immutable collections with perma.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public abstract class MutableCollectionSerializer<C extends Collection<T>, T>  implements KeyOrValueSerializer<C> {
    private final KeyOrValueSerializer<T> itemSerializer;

    public MutableCollectionSerializer(KeyOrValueSerializer<T> itemSerializer) {
        this.itemSerializer = itemSerializer;
    }

    @Override
    public byte[] toByteArray(C collection) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeInt(collection.size());
        for (T item : collection) {
            writer.writeWithLength(itemSerializer.toByteArray(item));
        }
        return writer.toByteArray();
    }

    @Override
    public C fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        int collectionSize = reader.readInt();
        C collection = createCollection(collectionSize);
        for (int i = 0; i < collectionSize; i++) {
            collection.add(itemSerializer.fromByteArray(reader.readWithLength()));
        }
        return collection;
    }

    protected abstract C createCollection(int size);
}
