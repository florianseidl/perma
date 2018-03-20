/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import ch.sbb.perma.FileRenameException;
import ch.sbb.perma.serializers.KeyOrValueSerializer;
import ch.sbb.perma.serializers.NullValueSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * The binary representation of a map or a delta to a map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class MapFileData<K,V> {
    private final Header header;
    private final ImmutableMap<K,V> newAndUpdated;
    private final ImmutableSet<K> deleted;

    MapFileData(Header mapFileHeader, ImmutableMap<K,V> newAndUpdated, ImmutableSet<K> deleted) {
        this.header = mapFileHeader;
        this.newAndUpdated = newAndUpdated;
        this.deleted = deleted;
    }

    public static <K,V> MapFileData<K,V> createNewFull(String name, ImmutableMap<K,V> current) {
        return new MapFileData<>(Header.newFullHeader(name, current.size()), current, ImmutableSet.of());
    }

    public static <K,V> MapFileData<K,V> readFileGroupAndCollect(File fullFile,
                                                                 List<File> deltaFiles,
                                                                 KeyOrValueSerializer<K> keySerializer,
                                                                 KeyOrValueSerializer<V> valueSerializer,
                                                                 Map<K,V> collector) throws IOException {
        MapFileData<K,V> latestData = MapFileData.readFrom(fullFile, keySerializer, valueSerializer)
                    .addTo(collector);
        if(!latestData.header.isFullFile()) {
            throw new HeaderMismatchException(
                    String.format("Invalid header, expected full file header but is %s",
                                    latestData.header));

        }
        latestData = readDeltaFilesAndCollect(deltaFiles, keySerializer, valueSerializer, latestData, collector);
        return latestData;
    }

    private static <K, V> MapFileData<K, V> readDeltaFilesAndCollect(List<File> deltaFiles,
                                                                     KeyOrValueSerializer<K> keySerializer,
                                                                     KeyOrValueSerializer<V> valueSerializer,
                                                                     MapFileData<K, V> previousData,
                                                                     Map<K, V> collector) throws IOException {
        MapFileData<K,V> latestData = previousData;
        for(File deltaFile : deltaFiles) {
            MapFileData<K,V> next = MapFileData
                    .readFrom(deltaFile, keySerializer, valueSerializer)
                    .addTo(collector);
            if (!next.header.isNextDeltaFileOf(latestData.header)) {
                throw new HeaderMismatchException(
                        String.format("Invalid header sequence, %s is not next delta of %s",
                                      next.header, latestData.header));

            }
            latestData = next;
        }
        return latestData;
    }

    private static <K,V> MapFileData<K,V> readFrom(File file,
                                                   KeyOrValueSerializer<K> keySerializer,
                                                   KeyOrValueSerializer<V> valueSerializer) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            return readFrom(in, keySerializer, valueSerializer);
        }
    }


    static <K,V> MapFileData<K,V> readFrom(InputStream input,
                                           KeyOrValueSerializer<K> keySerializer,
                                           KeyOrValueSerializer<V> valueSerializer) throws IOException {
        ImmutableMap.Builder<K,V> newOrUpdated = new ImmutableMap.Builder<>();
        ImmutableSet.Builder<K> deleted = new ImmutableSet.Builder<>();
        try (BufferedInputStream in = new BufferedInputStream(input)) {
            Header header = Header.readFrom(in);
            int count = 0;
            while (true) {
                MapEntryRecord<K,V> record = MapEntryRecord.readFrom(in, keySerializer, valueSerializer);
                if (record == null) {
                    break; // EOF
                }
                record.addTo(newOrUpdated, deleted);
                count++;
            }
            if(!header.hasSize(count)) {
                throw new HeaderMismatchException("Invalid size, mismatch between header and stored size");
            }
            return new MapFileData<>(header, newOrUpdated.build(), deleted.build());
        }
    }

    public MapFileData<K,V> updateWithDeltasAndCollect(List<File> additionalDeltaFiles,
                                                       KeyOrValueSerializer<K> keySerializer,
                                                       KeyOrValueSerializer<V> valueSerializer,
                                                       Map<K,V> collector) throws IOException {
        return readDeltaFilesAndCollect(additionalDeltaFiles, keySerializer, valueSerializer, this, collector);
    }

    public MapFileData<K,V> writeTo(File file,
                                    File tempFile, KeyOrValueSerializer<K> keySerializer,
                                    KeyOrValueSerializer<V> valueSerializer) throws IOException {
        MapFileData<K,V> mapFileData = writeToTempFile(tempFile, keySerializer, valueSerializer);
        if(!tempFile.renameTo(file)) {
            throw new FileRenameException(String.format("Could not rename temporary file %s to perma set file %s",
                    tempFile,
                    file));
        }
        return mapFileData;
    }

    private MapFileData<K,V> writeToTempFile(File tempFile,
                                             KeyOrValueSerializer<K> keySerializer,
                                             KeyOrValueSerializer<V> valueSerializer) throws IOException {
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            return writeTo(out, keySerializer, valueSerializer);
        }
    }

    MapFileData<K,V> writeTo(OutputStream output,
                             KeyOrValueSerializer<K> keySerializer,
                             KeyOrValueSerializer<V> valueSerializer) throws IOException {
        try (OutputStream out = new BufferedOutputStream(output)) {
            header.writeTo(out);
            for(Map.Entry<K,V> entry : newAndUpdated.entrySet()) {
                MapEntryRecord
                        .newOrUpdated(entry.getKey(), entry.getValue())
                        .writeTo(out, keySerializer, valueSerializer);
            }
            for(K deleted : deleted) {
                MapEntryRecord
                        .deleted(deleted)
                        .writeTo(out, keySerializer, NullValueSerializer.NULL);
            }
            out.flush();
            if(!header.hasSize(newAndUpdated.size() + deleted.size())) {
                throw new HeaderMismatchException("Invalid size, mismatch between header and stored size");
            }
        }
        return this;
    }

    public MapFileData<K,V> nextDelta(ImmutableMap<K,V> newAndUpdated, ImmutableSet<K> deleted) {
        return new MapFileData<>(header.nextDelta(newAndUpdated.size() + deleted.size()), newAndUpdated, deleted);
    }

    MapFileData<K,V> addTo(Map<K,V> map) {
        map.putAll(newAndUpdated);
        deleted.forEach(map::remove);
        return this;
    }

    @Override
    public String toString() {
        return "MapFileData{" +
                "header=" + header +
                ", newAndUpdated=" + newAndUpdated +
                ", deleted=" + deleted +
                '}';
    }
}
