/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

/**
 * Knows how to serialize and deserialize null values (set values or deleted record values).
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class NullValueSerializer implements KeyOrValueSerializer<Object> {
    public final static Object NULL_OBJECT = Boolean.TRUE;

    public final static NullValueSerializer NULL = new NullValueSerializer();

    private NullValueSerializer() {
    }

    @Override
    public byte[] toByteArray(Object dummy) {
        return null;
    }

    @Override
    public Object fromByteArray(byte[] bytes) {
        return NULL_OBJECT;
    }
}
