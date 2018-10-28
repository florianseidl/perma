/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import ch.sbb.perma.file.PermaFile;
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

    private MapFileData(Header header, ImmutableMap<K, V> newAndUpdated, ImmutableSet<K> deleted) {
        this.header = header;
        this.newAndUpdated = newAndUpdated;
        this.deleted = deleted;
    }

    public static <K,V> MapFileData<K,V> createNewFull(String name, ImmutableMap<K, V> current) {
        return new MapFileData<>(
                Header.newFullHeader(name, current.size()),
                current,
                ImmutableSet.of()
        );
    }

    public static <K,V> MapFileData<K,V> readFileGroupAndCollect(PermaFile fullFile,
                                                                 List<PermaFile> deltaFiles,
                                                                 KeyOrValueSerializer<K> keySerializer,
                                                                 KeyOrValueSerializer<V> valueSerializer,
                                                                 Map<K, V> collector) throws IOException {
        MapFileData<K,V> latestData = MapFileData.readFrom(fullFile, keySerializer, valueSerializer)
                    .addTo(collector);
        if(!latestData.header.isFullFile()) {
            throw new HeaderMismatchException(
                    String.format("Invalid header, expected full file header but is %s",
                                    latestData.header));

        }
        latestData = readDeltaFilesAndCollect(
                                    deltaFiles,
                                    keySerializer,
                                    valueSerializer,
                                    latestData,
                                    collector);
        return latestData;
    }

    private static <K, V> MapFileData<K, V> readDeltaFilesAndCollect(List<PermaFile> deltaFiles,
                                                                     KeyOrValueSerializer<K> keySerializer,
                                                                     KeyOrValueSerializer<V> valueSerializer,
                                                                     MapFileData<K, V> previousData,
                                                                     Map<K, V> collector) throws IOException {
        MapFileData<K,V> latestData = previousData;
        for(PermaFile deltaFile : deltaFiles) {
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

    private static <K,V> MapFileData<K,V> readFrom(PermaFile file,
                                                   KeyOrValueSerializer<K> keySerializer,
                                                   KeyOrValueSerializer<V> valueSerializer) throws IOException {
        return file.withInputStream(in -> readFrom(in, keySerializer, valueSerializer));
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

    public MapFileData<K,V> updateWithDeltasAndCollect(List<PermaFile> additionalDeltaFiles,
                                                       KeyOrValueSerializer<K> keySerializer,
                                                       KeyOrValueSerializer<V> valueSerializer,
                                                       Map<K,V> collector) throws IOException {
        return readDeltaFilesAndCollect(additionalDeltaFiles,
                                        keySerializer,
                                        valueSerializer,
                                        this,
                                        collector);
    }

    public MapFileData<K,V> writeTo(PermaFile targetFile,
                                    KeyOrValueSerializer<K> keySerializer,
                                    KeyOrValueSerializer<V> valueSerializer) throws IOException {
        return targetFile.withOutputStream(out -> writeTo(out, keySerializer, valueSerializer));
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
        return new MapFileData<>(
                        header.nextDelta(newAndUpdated.size() + deleted.size()),
                        newAndUpdated,
                        deleted
        );
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
