/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.KeyOrValueSerializer;
/**
 * Knows how to serialize and deserialize set values (null values).
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class SetValueSerializer implements KeyOrValueSerializer<Object> {
    public final static Object NULL_OBJECT = new Object();

    final static SetValueSerializer TO_NULL = new SetValueSerializer();

    private SetValueSerializer() {
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
