/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class ByteSerializer implements KeyOrValueSerializer<Byte> {
    @Override
    public byte[] toByteArray(Byte byteValue) {
        return new byte[]{byteValue};
    }

    @Override
    public Byte fromByteArray(byte[] bytes) {
        return bytes[0];
    }
}
