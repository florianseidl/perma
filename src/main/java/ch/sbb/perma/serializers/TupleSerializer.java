/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import org.javatuples.Tuple;

/**
 * Base javatuples tuples serializers and deserializers.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public abstract class TupleSerializer<T extends Tuple> implements KeyOrValueSerializer<T> {
    private final KeyOrValueSerializer[] serializers;

    protected TupleSerializer(KeyOrValueSerializer... serializers) {
        if(serializers == null) {
            throw new NullPointerException("Serializers are null");
        }
        this.serializers = serializers;
    }

    @Override
    public byte[] toByteArray(T tuple) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        for(int i = 0; i < tuple.getSize(); i++) {
            writer.writeWithLength(tuple.getValue(i) != null ?
                    toByteArray(serializers[i], tuple.getValue(i)) : null);
        }
        return writer.toByteArray();
    }

    private static byte[] toByteArray(KeyOrValueSerializer serializer, Object value) {
        byte[] bytes = serializer.toByteArray(value);
        if(bytes == null) {
            throw new IllegalArgumentException(String.format(
                    "To Null Serializer is not allowed as value serializer in Tuple serializer: %s",
                    serializer.getClass().getSimpleName()));
        }
        return bytes;
    }

    @Override
    public T fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        Object[] values = new Object[serializers.length];
        for(int i = 0; i < serializers.length; i++) {
            byte[] valueAsBytes = reader.readWithLength();
            values[i] = valueAsBytes != null ?
                    serializers[i].fromByteArray(valueAsBytes) : null;
        }
        return createFrom(values);
    }

    abstract T createFrom(Object[] values);
}
