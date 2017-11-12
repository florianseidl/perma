/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.time.LocalTime;

/**
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class LocalTimeSerializer implements KeyOrValueSerializer<LocalTime> {

    @Override
    public byte[] toByteArray(LocalTime localTime) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeByte(localTime.getHour());
        writer.writeByte(localTime.getMinute());
        writer.writeByte(localTime.getSecond());
        writer.writeInt(localTime.getNano());
        return writer.toByteArray();
    }

    @Override
    public LocalTime fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        return LocalTime.of(
                reader.readByte(),
                reader.readByte(),
                reader.readByte(),
                reader.readInt());
    }
}
