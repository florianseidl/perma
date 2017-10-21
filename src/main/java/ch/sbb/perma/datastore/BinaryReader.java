/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

/**
 * Helper methods to write and read bytes from and to a stream.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class BinaryReader {
    private final static int NULL_LENGTH = -1;

    private final InputStream in;
    private final Checksum checksum;

    public BinaryReader(InputStream in, Checksum crc32) {
        this.in = in;
        this.checksum = crc32;
    }

    public BinaryReader(InputStream in) {
        this.in = in;
        this.checksum = NoChecksum.INSTANCE;
    }

    byte[] readWithLength() throws IOException {
        int length = readInt();
        if(length == NULL_LENGTH) {
            return null;
        }
        return read(length);
    }

    int readByte() throws IOException {
        int value = in.read();
        checksum.update(value);
        return value;
    }

    int readInt() throws IOException {
        return Ints.fromByteArray(read(4));
    }

    long readLong() throws IOException {
        return Longs.fromByteArray(read(8));
    }

    byte[] read(int length) throws IOException {
        byte[] bytes = new byte[length];
        in.read(bytes);
        checksum.update(bytes, 0, length);
        return bytes;
    }

    boolean readAndCheckChecksum() throws IOException {
        return checksum.getValue() == new BinaryReader(in).readLong();
    }
}
