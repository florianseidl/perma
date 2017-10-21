/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * The binary representation of a map or a delta to a map.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class MapData<K,V> {
    private final Header header;
    private final ImmutableMap<K,V> newAndUpdated;
    private final ImmutableSet<K> deleted;

    private MapData(Header mapFileHeader, ImmutableMap<K,V> newAndUpdated, ImmutableSet<K> deleted) {
        this.header = mapFileHeader;
        this.newAndUpdated = newAndUpdated;
        this.deleted = deleted;
    }

    public static <K,V> MapData<K,V> createNewFull(String name, ImmutableMap<K,V> current) {
        return new MapData<K,V>(Header.newFullHeader(name), current, ImmutableSet.of());
    }

    public static <K,V> MapData<K,V> readAllAndCollect(File fullFile,
                                                List<File> deltaFiles,
                                                KeyOrValueSerializer<K> keySerializer,
                                                KeyOrValueSerializer<V> valueSerializer,
                                                Map<K,V> collector) throws IOException {
        MapData<K,V> latestData = MapData.readFrom(fullFile, keySerializer, valueSerializer)
                .addTo(collector);
        if(!latestData.header.isFullFile()) {
            throw new HeaderMismatchException(
                    String.format("Invalid header, expected full file header but is %s",
                                    latestData.header));

        }
        for(File deltaFile : deltaFiles) {
            MapData next = MapData
                    .readFrom(deltaFile, keySerializer, valueSerializer)
                    .addTo(collector);
            if(!next.header.isNextDeltaFileOf(latestData.header)) {
                throw new HeaderMismatchException(
                        String.format("Invalid header sequence, %s is not next delta of %s",
                                        next.header, latestData.header));

            }
            latestData = next;
        }
        return latestData;
    }

    private static <K,V> MapData<K,V> readFrom(File file,
                                       KeyOrValueSerializer<K> keySerializer,
                                       KeyOrValueSerializer<V> valueSerializer) throws IOException {
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            return readFrom(in, keySerializer, valueSerializer);
        }
    }

    static <K,V> MapData<K,V> readFrom(InputStream input,
                                       KeyOrValueSerializer<K> keySerializer,
                                       KeyOrValueSerializer<V> valueSerializer) throws IOException {
        ImmutableMap.Builder<K,V> newOrUpdated = new ImmutableMap.Builder<>();
        ImmutableSet.Builder<K> deleted = new ImmutableSet.Builder<>();
        try (BufferedInputStream in = new BufferedInputStream(input)) {
            Header header = Header.readFrom(in);
            while (true) {
                MapEntryRecord record = MapEntryRecord.readFrom(in, keySerializer, valueSerializer);
                if (record == null) {
                    break; // EOF
                }
                record.addTo(newOrUpdated, deleted);
            }
            return new MapData(header, newOrUpdated.build(), deleted.build());
        }
    }

    public MapData<K,V> writeTo(File file,
                         KeyOrValueSerializer<K> keySerializer,
                         KeyOrValueSerializer<V> valueSerializer) throws IOException {
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
            return writeTo(out, keySerializer, valueSerializer);
        }
    }

    MapData<K,V> writeTo(OutputStream output,
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
                        .writeTo(out, keySerializer, valueSerializer);
            }
            out.flush();
        }
        return this;
    }

    public MapData<K,V> nextDelta(ImmutableMap<K,V> newAndUpdated, ImmutableSet<K> deleted) {
        return new MapData(header.nextDelta(), newAndUpdated, deleted);
    }

    MapData<K,V> addTo(Map<K,V> map) {
        map.putAll(newAndUpdated);
        deleted.forEach(map::remove);
        return this;
    }

    @Override
    public String toString() {
        return "MapData{" +
                "header=" + header +
                ", newAndUpdated=" + newAndUpdated +
                ", deleted=" + deleted +
                '}';
    }
}
