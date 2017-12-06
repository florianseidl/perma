/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.math.BigInteger;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class BigIntegerSerializer implements KeyOrValueSerializer<BigInteger> {

    @Override
    public byte[] toByteArray(BigInteger bigInteger) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeWithLength(bigInteger.toByteArray());
        return writer.toByteArray();
    }

    @Override
    public BigInteger fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        return new BigInteger(reader.readWithLength());
    }
}
