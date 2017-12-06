/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Longs;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class DoubleSerializer implements KeyOrValueSerializer<Double> {

    @Override
    public byte[] toByteArray(Double doubleValue) {
        return Longs.toByteArray(Double.doubleToRawLongBits(doubleValue));
    }

    @Override
    public Double fromByteArray(byte[] bytes) {
        return Double.longBitsToDouble(Longs.fromByteArray(bytes));
    }
}
