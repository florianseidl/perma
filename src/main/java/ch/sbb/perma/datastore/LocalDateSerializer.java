/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.time.LocalDate;

/**
 * Serializer a LocalDate.
 *
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class LocalDateSerializer implements KeyOrValueSerializer<LocalDate> {

    @Override
    public byte[] toByteArray(LocalDate localDate) {
        CompoundBinaryWriter writer = new CompoundBinaryWriter();
        writer.writeInt(localDate.getYear());
        writer.writeByte((byte)localDate.getMonthValue());
        writer.writeByte((byte)localDate.getDayOfMonth());
        return writer.toByteArray();
    }

    @Override
    public LocalDate fromByteArray(byte[] bytes) {
        CompoundBinaryReader reader = new CompoundBinaryReader(bytes);
        return LocalDate.of(
                reader.readInt(),
                reader.readByte(),
                reader.readByte());
    }
}
