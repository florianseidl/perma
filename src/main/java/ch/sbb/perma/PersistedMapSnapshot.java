/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.MapFileData;
import ch.sbb.perma.file.FileGroup;
import ch.sbb.perma.file.PermaFile;
import ch.sbb.perma.serializers.KeyOrValueSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A snapshot of a map that is persisted to Writable files.
 * <p>
 *      Load and store a complete snapshot of the map.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class PersistedMapSnapshot<K,V> implements MapSnapshot<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(MapSnapshot.class);

    private final String name;
    private final FileGroup files;
    private final Options options;
    private final ImmutableMap<K,V> mapSnapshot;
    private final MapFileData<K,V> persited;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    PersistedMapSnapshot(String name,
                         FileGroup files,
                         Options options,
                         ImmutableMap<K, V> mapSnapshot,
                         MapFileData<K,V> persited,
                         KeyOrValueSerializer<K> keySerializer,
                         KeyOrValueSerializer<V> valueSerializer) {
        this.name = name;
        this.files = files;
        this.options = options;
        this.mapSnapshot = mapSnapshot;
        this.persited = persited;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    static <K,V> MapSnapshot<K,V> load(String permaName,
                                       FileGroup latestFiles,
                                       Options options,
                                       KeyOrValueSerializer<K> keySerializer,
                                       KeyOrValueSerializer<V> valueSerializer) throws IOException{
        LOG.debug("Loading persisted Snapshot from files latestFiles {}", latestFiles);
        Map<K,V> collector = new HashMap<>();
        MapFileData<K,V> latestData = MapFileData.readFileGroupAndCollect(
                latestFiles.fullFile(),
                latestFiles.deltaFiles(),
                keySerializer,
                valueSerializer,
                collector);
        return new PersistedMapSnapshot<>(
                permaName,
                latestFiles,
                options,
                ImmutableMap.copyOf(collector),
                latestData,
                keySerializer,
                valueSerializer);
    }

    @Override
    public MapSnapshot<K,V> writeNext(Map<K,V> current) throws IOException {
        return writeNext(ImmutableMap.copyOf(current));
    }

    private MapSnapshot<K,V> writeNext(ImmutableMap<K,V> currentImmutable) throws IOException {
        MapDifference<K,V> diff = Maps.difference(mapSnapshot, currentImmutable);
        if(diff.areEqual()) {
            LOG.debug("Noting to write (no changes detected), ignoring");
            return this;
        }
        if(moreThanAThirdUpdatedOrDeleted(diff)) {
            LOG.debug("More than a third of the records are added and/or deleted, compacting to full file");
            return compactTo(currentImmutable);
        }
        FileGroup filesWithNextDeltaFile = files.withNextDelta();
        LOG.debug("Writing delta to file {} after deleting stale temp files", filesWithNextDeltaFile.latestDeltaFile());
        MapFileData<K,V> nextDeltaData = toDelta(diff).writeTo(
                                                filesWithNextDeltaFile.latestDeltaFile(),
                                                keySerializer,
                                                valueSerializer);
        return new PersistedMapSnapshot<>(
                                name,
                                filesWithNextDeltaFile,
                                options,
                                currentImmutable,
                                nextDeltaData,
                                keySerializer,
                                valueSerializer);
    }

    private boolean moreThanAThirdUpdatedOrDeleted(MapDifference<K,V> diff) {
        return ((double)diff.entriesOnlyOnLeft().size() + diff.entriesDiffering().size())
                > (mapSnapshot.size() / 3.0);
    }

    @Override
    public MapSnapshot<K, V> refresh() throws IOException {
        FileGroup refreshedFiles = files.refresh();
        if(!refreshedFiles.hasSameFullFileAs(files)) { // there was a compact, reload
            LOG.debug("Reloading instead of refresh, full file has changed");
            return load(name, refreshedFiles, options, keySerializer, valueSerializer);
        }
        List<PermaFile> additionalDeltaFiles = refreshedFiles.deltaFilesSince(files);
        if(additionalDeltaFiles.isEmpty()) {
            LOG.debug("No new files found, cancelling refresh");
            return this;
        }
        LOG.debug("Refreshing from files {}", additionalDeltaFiles);
        Map<K,V> collector = new HashMap<>(mapSnapshot);
        MapFileData<K,V> lastData = persited.updateWithDeltasAndCollect(
                                additionalDeltaFiles,
                                keySerializer,
                                valueSerializer,
                                collector);
        return new PersistedMapSnapshot<>(
                                name,
                                refreshedFiles,
                                options,
                                ImmutableMap.copyOf(collector),
                                lastData,
                                keySerializer,
                                valueSerializer);
    }

    @Override
    public MapSnapshot<K, V> compact() throws IOException {
        return compactTo(mapSnapshot);
    }

    private MapSnapshot<K, V> compactTo(ImmutableMap<K,V> nextMapSnapshot) throws IOException {
        LOG.debug("Compacting map snapshot files {}", files);
        MapSnapshot<K,V> compactedSnapshot = new NewMapSnapshot<>(name, files, options, keySerializer, valueSerializer)
                .writeNext(nextMapSnapshot);
        LOG.debug("Deleting files {}", files);
        files.delete();
        return compactedSnapshot;
    }

    @Override
    public ImmutableMap<K,V> asImmutableMap() {
        return mapSnapshot;
    }

    private MapFileData<K,V> toDelta(MapDifference<K,V> diff) {
        Set<K> deleted = diff.entriesOnlyOnLeft().keySet();
        Map<K,V> newEntries = diff.entriesOnlyOnRight();
        Map<K,V> modifiedEntries =
                diff.entriesDiffering().entrySet().stream()
                        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().rightValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        modifiedEntries.putAll(newEntries);
        LOG.debug("Delta with newAndModifiedEnties.size={} and deleted.size={}",
                        modifiedEntries.size(), deleted.size());
        return persited.nextDelta(ImmutableMap.copyOf(modifiedEntries), ImmutableSet.copyOf(deleted));
    }

}
