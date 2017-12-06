/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Shorts;

/**
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class ShortSerializer implements KeyOrValueSerializer<Short> {
    @Override
    public byte[] toByteArray(Short shortValue) {
        return Shorts.toByteArray(shortValue);
    }

    @Override
    public Short fromByteArray(byte[] bytes) {
        return Shorts.fromByteArray(bytes);
    }
}
