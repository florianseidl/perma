/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import ch.sbb.perma.datastore.BinaryReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Read multiple items from bytes.
 *
 * @author u206123 (Florian Seidl)
 * @since 2.1, 2017.
 */
public class CompoundBinaryReader {
    private final BinaryReader reader;
    public CompoundBinaryReader(byte[] bytes) {
        reader = new BinaryReader(new ByteArrayInputStream(bytes));
    }

    public byte[] readWithLength() {
        try {
            return reader.readWithLength();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public int readInt() {
        try {
            return reader.readInt();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public int readByte() {
        try {
            return reader.readByte();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public short readShort() {
        try {
            return reader.readShort();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }


    public long readLong() {
        try {
            return reader.readLong();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
