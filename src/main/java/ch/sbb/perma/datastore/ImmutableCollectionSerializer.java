/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.collect.ImmutableCollection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serialize a immutable collection as value. Each item is serialized using the given item serializer.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
abstract class ImmutableCollectionSerializer<C extends ImmutableCollection<T>,T> implements KeyOrValueSerializer<C>{
    private KeyOrValueSerializer<T> itemSerializier;

    public ImmutableCollectionSerializer(KeyOrValueSerializer<T> itemSerializier) {
        this.itemSerializier = itemSerializier;
    }

    @Override
    public byte[] toByteArray(C collection) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            BinaryWriter writer = new BinaryWriter(bos);
            writer.writeInt(collection.size());
            for(T item : collection) {
                writer.writeWithLength(itemSerializier.toByteArray(item));
            }
            bos.flush();
            return bos.toByteArray();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    public C fromByteArray(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            BinaryReader reader = new BinaryReader(bis);
            int collectionSize = reader.readInt();
            ImmutableCollection.Builder<T> builder = collectionBuilder();
            for(int i = 0; i < collectionSize; i++) {
                builder.add(itemSerializier.fromByteArray(reader.readWithLength()));
            }
            return (C) builder.build();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    abstract ImmutableCollection.Builder<T> collectionBuilder();
}
