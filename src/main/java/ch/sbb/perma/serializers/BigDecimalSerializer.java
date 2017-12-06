/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class BigDecimalSerializer implements KeyOrValueSerializer<BigDecimal> {
    @Override
    public byte[] toByteArray(BigDecimal bigDecimal) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeWithLength(bigDecimal.unscaledValue().toByteArray());
        writer.writeInt(bigDecimal.scale());
        return writer.toByteArray();
    }

    @Override
    public BigDecimal fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        return new BigDecimal(new BigInteger(reader.readWithLength()), reader.readInt());
    }
}
