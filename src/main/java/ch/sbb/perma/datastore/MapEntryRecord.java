/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * A single record for a map entry.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class MapEntryRecord<K,V> {
    private abstract static class RecordType {
        private final static Set <RecordType> values = new HashSet<>();
        private final byte byteValue;

        RecordType(int byteValue) {
            this.byteValue = (byte) byteValue;
            values.add(this);
        }

        static MapEntryRecord.RecordType of(int byteValue) {
            return RecordType.values
                    .stream()
                    .filter(type -> type.byteValue == byteValue)
                    .findFirst()
                    .orElseThrow(() -> new InvalidDataException(String.format("Invalid RecordType byte code: %d", byteValue)));
        }

        abstract <K, V> void addRecordTo(MapEntryRecord<K,V> record,
                                  ImmutableMap.Builder<K, V> addedUpdatedEntries,
                                  ImmutableSet.Builder<K> deletedEntries);

        abstract <K,V> MapEntryRecord<K,V> createRecord(byte[] keyAsBytes,
                                                  byte[] valueAsBytes,
                                                  KeyOrValueSerializer<K> keySerializer,
                                                  KeyOrValueSerializer<V> valueSerializer);
        @Override
        public String toString() {
            return Byte.toString(byteValue);
        }
    }
    private final static RecordType NEW_UPDATED = new RecordType(0) {
        @Override
        <K, V> void addRecordTo(MapEntryRecord<K,V> record,
                                ImmutableMap.Builder<K, V> addedUpdatedEntries,
                                ImmutableSet.Builder<K> deletedEntries) {
            addedUpdatedEntries.put(record.key, record.value);
        }

        @Override
        <K, V> MapEntryRecord<K, V> createRecord(byte[] keyAsBytes,
                                                 byte[] valueAsBytes,
                                                 KeyOrValueSerializer<K> keySerializer,
                                                 KeyOrValueSerializer<V> valueSerializer) {
            return new MapEntryRecord<>(
                    keySerializer.fromByteArray(keyAsBytes),
                    valueSerializer.fromByteArray(valueAsBytes),
                    this);
        }
    };
    private final static RecordType DELETED = new RecordType(1) {
        @Override
        <K, V> void addRecordTo(MapEntryRecord<K,V> record,
                                ImmutableMap.Builder<K, V> addedUpdatedEntries,
                                ImmutableSet.Builder<K> deletedEntries) {
            deletedEntries.add(record.key);
        }

        @Override
        <K, V> MapEntryRecord<K, V> createRecord(byte[] keyAsBytes,
                                                 byte[] valueAsBytes,
                                                 KeyOrValueSerializer<K> keySerializer,
                                                 KeyOrValueSerializer<V> valueSerializer) {
            return new MapEntryRecord<>(
                    keySerializer.fromByteArray(keyAsBytes),
                    null,
                    this);
        }
    };
    private static final int MARKER = 0xF5;
    private static final int EOF = -1;

    private final K key;
    private final V value;
    private final RecordType recordType;

    private MapEntryRecord(K key, V value, RecordType recordType) {
        this.key = key;
        this.value = value;
        this.recordType = recordType;
    }

    static <K,V> MapEntryRecord<K,V> newOrUpdated(K key, V value) {
        return new MapEntryRecord<>(key, value, NEW_UPDATED);
    }

    static <K,V> MapEntryRecord<K,V> deleted(K key) {
        return new MapEntryRecord<>(key, null, DELETED);
    }

    static <K,V> MapEntryRecord<K,V> readFrom(InputStream in,
                                              KeyOrValueSerializer<K> keySerializer,
                                              KeyOrValueSerializer<V> valueSerializer) throws IOException {
        int marker = new BinaryReader(in).readByte();
        if (marker == EOF) {
            return null;
        }
        if (marker != MARKER) {
            throw new InvalidDataException(String.format("Invalid record marker: %x", marker));
        }
        BinaryReader readerWithChecksum = new BinaryReader(in, new CRC32());
        RecordType recordType = RecordType.of(readerWithChecksum.readByte());
        byte[] keyAsBytes = readerWithChecksum.readWithLength();
        byte[] valueAsBytes = readerWithChecksum.readWithLength();
        if (!readerWithChecksum.readAndCheckChecksum()) {
            throw new InvalidDataException("Record checksum mismatch");
        }
        return recordType.createRecord(keyAsBytes, valueAsBytes, keySerializer, valueSerializer);
    }

    void writeTo(OutputStream out,
                 KeyOrValueSerializer<K> keySerializer,
                 KeyOrValueSerializer<V> valueSerializer) throws IOException {
        new BinaryWriter(out).writeByte(MARKER);
        BinaryWriter writerWithChecksum = new BinaryWriter(out, new CRC32());
        writerWithChecksum.writeByte(recordType.byteValue);
        writerWithChecksum.writeWithLength(keySerializer.toByteArray(key));
        writerWithChecksum.writeWithLength(valueSerializer.toByteArray(value));
        writerWithChecksum.writeChecksum();
    }

    void addTo(ImmutableMap.Builder<K, V> addedUpdatedEntries,
               ImmutableSet.Builder<K> deletedEntries) {
        recordType.addRecordTo(
                this,
                addedUpdatedEntries,
                deletedEntries);
    }

    @Override
    public String toString() {
        return "MapEntryRecord{" +
                "key=" + key +
                ", value=" + value +
                ", recordType=" + recordType +
                '}';
    }
}
