/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Writable files in a directory. Can List, create new files,...
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class FileGroup {
    private final File dir;
    private final String permaName;
    private final PermaFile fullFile;
    private final ImmutableList<PermaFile> deltaFiles;

    private FileGroup(File dir, String permaName, PermaFile fullFile, ImmutableList<PermaFile> deltaFiles) {
        this.dir = dir;
        this.permaName = permaName;
        this.fullFile = fullFile;
        this.deltaFiles = deltaFiles;
    }

    public static FileGroup list(File dir, String name) {
        return new FullFilePattern(name).latestFullFile(dir)
                .map(latestFullFileName -> new FileGroup(dir,
                        name,
                        latestFullFileName,
                        latestFullFileName.deltaFileNamePattern().listDeltaFiles(dir)))
                .orElse(new FileGroup(dir, name, null, ImmutableList.of()));
    }

    public FileGroup refresh() {
        return list(dir, permaName);
    }

    public boolean exists() {
        return fullFile != null;
    }

    public PermaFile fullFile() throws FileNotFoundException {
        if (fullFile == null) {
            throw new FileNotFoundException(
                    String.format("No file for perma %s found in %s", permaName, dir));
        }
        return fullFile;
    }

    public boolean hasSameFullFileAs(FileGroup other) {
        return Objects.equals(fullFile, other.fullFile);
    }

    public PermaFile latestDeltaFile() throws FileNotFoundException {
        if (deltaFiles.isEmpty()) {
            throw new FileNotFoundException(
                    String.format("No delta file for perma %s found in %s", permaName, dir));
        }
        return deltaFiles.get(deltaFiles.size() - 1);
    }

    public List<PermaFile> deltaFiles() {
        return deltaFiles;
    }

    public List<PermaFile> deltaFilesSince(FileGroup previousFiles) {
        if (previousFiles.deltaFiles.isEmpty()) {
            return deltaFiles;
        }
        return deltaFiles.subList(
                previousFiles.deltaFiles.size(),
                deltaFiles.size());
    }

    public FileGroup withNextFull(Compression compression) {
        if (fullFile == null) {
            return new FileGroup(
                    dir,
                    permaName,
                    PermaFile.fullFile(compression, dir, permaName, 1),
                    ImmutableList.of());
        }
        return new FileGroup(
                dir,
                permaName,
                fullFile.nextFull(compression),
                ImmutableList.of());
    }

    public FileGroup withNextDelta() {
        return new FileGroup(dir,
                permaName,
                fullFile,
                ImmutableList.<PermaFile>builder()
                        .addAll(deltaFiles)
                        .add(nextDeltaFileName())
                        .build());
    }

    private PermaFile nextDeltaFileName() {
        if(deltaFiles.isEmpty()) {
            return fullFile.nextDelta();
        }
        return deltaFiles.get(deltaFiles.size() -1).nextDelta();
    }

    public void delete() throws IOException {
        if (!exists()) {
            return;
        }
        fullFile().delete();
        for (PermaFile deltaFile : deltaFiles()) {
            deltaFile.delete();
        }
    }

    @Override
    public String toString() {
        return "FileGroup{" +
                "dir=" + dir +
                ", permaName='" + permaName + '\'' +
                ", fullFile='" + fullFile + '\'' +
                ", deltaFiles=" + deltaFiles +
                '}';
    }
}
