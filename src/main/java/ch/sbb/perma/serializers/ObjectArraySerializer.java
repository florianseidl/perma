/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

import com.google.common.collect.ObjectArrays;

/**
 * Serialize an object array.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class ObjectArraySerializer<T> implements KeyOrValueSerializer<T[]> {
    private final Class<T> elementClass;
    private final KeyOrValueSerializer<T> elementSerializer;

    public ObjectArraySerializer(Class<T> elementClass, KeyOrValueSerializer<T> elementSerializer) {
        this.elementClass = elementClass;
        this.elementSerializer = elementSerializer;
    }

    @Override
    public byte[] toByteArray(T[] array) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeInt(array.length);
        for(T element : array) {
            writer.writeWithLength(elementSerializer.toByteArray(element));
        }
        return writer.toByteArray();
    }

    @Override
    public T[] fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        int length = reader.readInt();
        T[] array = ObjectArrays.newArray(elementClass, length);
        for(int i = 0; i < length; i++) {
            array[i] = elementSerializer.fromByteArray(reader.readWithLength());
        }
        return array;
    }
}
