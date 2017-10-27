/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import org.javatuples.Tuple;

/**
 * Base javatuples tuples serializers and deserializers.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public abstract class TupleSerializer<T extends Tuple> implements KeyOrValueSerializer<T> {
    private final KeyOrValueSerializer[] serializers;

    protected TupleSerializer(KeyOrValueSerializer[] serializers) {
        this.serializers = serializers;
    }

    @Override
    public byte[] toByteArray(T tuple) {
        CollectionBinaryWriter writer = new CollectionBinaryWriter();
        for(int i = 0; i < tuple.getSize(); i++) {
            writer.writeNext(tuple.getValue(i) != null ?
                    serializers[i].toByteArray(tuple.getValue(i)) : null);
        }
        return writer.toByteArray();
    }

    @Override
    public T fromByteArray(byte[] bytes) {
        CollectionBinaryReader reader = new CollectionBinaryReader(bytes);
        Object[] values = new Object[serializers.length];
        for(int i = 0; i < serializers.length; i++) {
            byte[] valueAsBytes = reader.readNext();
            values[i] = valueAsBytes != null ?
                    serializers[i].fromByteArray(valueAsBytes) : null;
        }
        return createFrom(values);
    }

    abstract T createFrom(Object[] values);
}
