/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Ints;

/**
 * Serialize an integer.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class IntegerSerializer implements KeyOrValueSerializer<Integer> {
    @Override
    public byte[] toByteArray(Integer integer) {
        return Ints.toByteArray(integer);
    }

    @Override
    public Integer fromByteArray(byte[] bytes) {
        return Ints.fromByteArray(bytes);
    }
}
