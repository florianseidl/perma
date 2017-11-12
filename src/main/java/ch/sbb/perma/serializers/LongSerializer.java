/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import com.google.common.primitives.Longs;

/**
 * Serialize a long.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class LongSerializer implements KeyOrValueSerializer<Long> {
    @Override
    public byte[] toByteArray(Long longValue) {
        return Longs.toByteArray(longValue);
    }

    @Override
    public Long fromByteArray(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }
}
