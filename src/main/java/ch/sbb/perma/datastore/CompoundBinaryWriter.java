/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static ch.sbb.perma.datastore.KeyOrValueSerializer.STRING;

/**
 * Write multiple items to bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class CompoundBinaryWriter {
    private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private final BinaryWriter writer = new BinaryWriter(bos);

    public void writeWithLength(byte[] bytes) {
        try {
            writer.writeWithLength(bytes);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void writeInt(int length) {
        try {
            writer.writeInt(length);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void writeByte(int byteValue) {
        try {
            writer.writeByte(byteValue);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void writeShort(short shortValue) {
        try {
            writer.writeShort(shortValue);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }


    public void writeLong(long longValue) {
        try {
            writer.writeLong(longValue);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void writeString(String value) {
        writeWithLength(STRING.toByteArray(value));
    }

    public byte[] toByteArray() {
        return bos.toByteArray();
    }
}
