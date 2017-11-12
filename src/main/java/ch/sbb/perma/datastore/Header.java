/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32;

import static ch.sbb.perma.serializers.KeyOrValueSerializer.STRING;

/**
 * The binary format of the header of a Writable File.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class Header {
    private static final short VERSION = 1;
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final byte[] FILE_MARKER = "PeMa".getBytes(UTF_8);

    private enum FileType {
        FULL((byte)0),
        DELTA((byte)1);

        private final byte byteValue;

        FileType(byte byteValue) {
            this.byteValue = byteValue;
        }

        static FileType of(int byteValue) {
            return Arrays.stream(Header.FileType.values())
                    .filter(type -> type.byteValue == byteValue)
                    .findFirst()
                    .orElseThrow(() -> new InvalidDataException(String.format("Invalid FileType byte code: %d", byteValue)));
        }
    }

    private final FileType fileType;
    private final UUID fullFileUUID;
    private final int updateFileNumber;
    private final int size;
    private final String name;

    Header(FileType fileType, UUID fullFileUUID, int updateFileNumber, int size, String name) {
        this.fileType = fileType;
        this.fullFileUUID = fullFileUUID;
        this.updateFileNumber = updateFileNumber;
        this.size = size;
        this.name = name;
    }

    static Header newFullHeader(String name, int size) {
        return new Header(FileType.FULL, UUID.randomUUID(), 0, size, name);
    }

    Header nextDelta(int size) {
        return new Header(FileType.DELTA, this.fullFileUUID, this.updateFileNumber + 1, size, name);
    }

    Header writeTo(OutputStream out) throws IOException {
        new BinaryWriter(out).write(FILE_MARKER);
        BinaryWriter writerWithChecksum = new BinaryWriter(out, new CRC32());
        writerWithChecksum.writeShort(VERSION);
        writerWithChecksum.writeByte(fileType.byteValue);
        writerWithChecksum.writeLong(fullFileUUID.getMostSignificantBits());
        writerWithChecksum.writeLong(fullFileUUID.getLeastSignificantBits());
        writerWithChecksum.writeInt(updateFileNumber);
        writerWithChecksum.writeWithLength(STRING.toByteArray(name));
        writerWithChecksum.writeInt(size);
        writerWithChecksum.writeChecksum();
        return this;
    }


    static Header readFrom(InputStream in) throws IOException {
        byte[] marker = new BinaryReader(in).read(FILE_MARKER.length);
        if(!Arrays.equals(marker, FILE_MARKER)) {
            throw new InvalidDataException(String.format("Not am Writable file, file marker invalid: %s", marker));
        }
        BinaryReader readerWithChecksum = new BinaryReader(in, new CRC32());
        readerWithChecksum.readShort(); // version is ignored for now
        FileType fileType = FileType.of(readerWithChecksum.readByte());
        UUID uuid = new UUID(readerWithChecksum.readLong(),
                             readerWithChecksum.readLong());
        int updateFileNumber = readerWithChecksum.readInt();
        String name = STRING.fromByteArray(readerWithChecksum.readWithLength());
        int size = readerWithChecksum.readInt();
        if(!readerWithChecksum.readAndCheckChecksum()) {
            throw new InvalidDataException(
                    String.format("Checksum mismatch in File header of header with name %.999s and uuid %s",
                                    name, uuid));
        }
        return new Header(fileType, uuid, updateFileNumber, size, name);
    }

    boolean isFullFile() {
        return fileType == FileType.FULL;
    }

    boolean belongsToSameFullFileAs(Header other) {
        return this.name.equals(other.name) && this.fullFileUUID.equals(other.fullFileUUID);
    }

    boolean isNextDeltaFileOf(Header other) {
        return !this.isFullFile() &&
                this.belongsToSameFullFileAs(other) &&
                this.updateFileNumber == other.updateFileNumber + 1;
    }

    boolean hasSize(int mapDataSize) {
        return mapDataSize == this.size;
    }

    @Override
    public String toString() {
        return "Header{" +
                "fileType=" + fileType +
                ", fullFileUUID=" + fullFileUUID +
                ", updateFileNumber=" + updateFileNumber +
                ", name='" + name + '\'' +
                '}';
    }
}
