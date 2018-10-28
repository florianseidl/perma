/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

/**
 * Read int, long, short and byte arrays with length from binary data.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class BinaryReader {
    private final static int NULL_LENGTH = -1;

    private final InputStream in;
    private final Checksum checksum;

    BinaryReader(InputStream in, Checksum crc32) {
        this.in = in;
        this.checksum = crc32;
    }

    public BinaryReader(InputStream in) {
        this.in = in;
        this.checksum = NoChecksum.INSTANCE;
    }

    public byte[] readWithLength() throws IOException {
        int length = readInt();
        if(length == NULL_LENGTH) {
            return null;
        }
        return read(length);
    }

    public int readByte() throws IOException {
        int value = in.read();
        checksum.update(value);
        return value;
    }

    public short readShort() throws IOException {
        return Shorts.fromByteArray(read(2));
    }

    public int readInt() throws IOException {
        return Ints.fromByteArray(read(4));
    }

    public long readLong() throws IOException {
        return Longs.fromByteArray(read(8));
    }

    public byte[] read(int length) throws IOException {
        byte[] bytes = new byte[length];
        if(length == 0) {
            return bytes;
        }
        if(in.read(bytes) != length) {
            throw new InvalidDataException("Less bytes available than expected");
        }
        checksum.update(bytes, 0, length);
        return bytes;
    }

    boolean readAndCheckChecksum() throws IOException {
        return checksum.getValue() == new BinaryReader(in).readLong();
    }
}
