/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * Write int, long, short and byte arrays with length to binary data.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class BinaryWriter {
    private final static int NULL_LENGTH = -1;

    private final OutputStream out;
    private final Checksum checksum;

    BinaryWriter(OutputStream out, Checksum checksum) {
        this.out = out;
        this.checksum = checksum;
    }

    public BinaryWriter(OutputStream out) {
        this.out = out;
        this.checksum = NoChecksum.INSTANCE;
    }

    public void writeWithLength(byte[] bytes) throws IOException {
        if(bytes == null) {
            writeInt(NULL_LENGTH);
            return;
        }
        writeInt(bytes.length);
        write(bytes);
    }

    public void writeByte(int value) throws IOException {
        checksum.update(value);
        out.write(value);
    }

    public void writeShort(short value) throws IOException {
        write(Shorts.toByteArray(value));
    }

    public void writeInt(int value) throws IOException {
        write(Ints.toByteArray(value));
     }

    public void writeLong(long value) throws IOException {
        write(Longs.toByteArray(value));
    }

    public void write(byte[] bytes) throws IOException {
        checksum.update(bytes, 0 , bytes.length);
        out.write(bytes);
    }

    void writeChecksum() throws IOException {
        new BinaryWriter(out).writeLong(checksum.getValue());
    }
}
