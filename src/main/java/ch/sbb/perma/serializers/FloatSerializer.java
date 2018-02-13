/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Ints;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class FloatSerializer implements KeyOrValueSerializer<Float> {

    @Override
    public byte[] toByteArray(Float floatValue) {
        return Ints.toByteArray(Float.floatToRawIntBits(floatValue));
    }

    @Override
    public Float fromByteArray(byte[] bytes) {
        return Float.intBitsToFloat(Ints.fromByteArray(bytes));
    }
}
