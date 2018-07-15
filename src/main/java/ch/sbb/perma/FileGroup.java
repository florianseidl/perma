/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.file.DeltaFilePattern;
import ch.sbb.perma.file.PermaFile;
import ch.sbb.perma.file.FullFilePattern;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Writable files in a directory. Can List, create new files,...
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class FileGroup {
    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String TEMP_FILE_PATTERN_TEMPLATE = String.format(TEMP_FILE_FORMAT, "%s", ".+");

    private final File dir;
    private final String name;
    private final PermaFile fullFile;
    private final ImmutableList<PermaFile> deltaFiles;

    private FileGroup(File dir, String name, PermaFile fullFile, ImmutableList<PermaFile> deltaFiles) {
        this.dir = dir;
        this.name = name;
        this.fullFile = fullFile;
        this.deltaFiles = deltaFiles;
    }

    public static FileGroup list(File dir, String name) {
        return latestFullFileName(dir, name)
                .map(latestFullFileName -> new FileGroup(dir,
                        name,
                        latestFullFileName,
                        latestFullFileName.deltaFileNamePattern().parse(listDir(dir, latestFullFileName.deltaFileNamePattern()))))
                .orElse(new FileGroup(dir, name, null, ImmutableList.of()));
    }

    FileGroup refresh() {
        return list(dir, name);
    }

    boolean exists() {
        return fullFile != null;
    }

    PermaFile fullFile() throws FileNotFoundException {
        if (fullFile == null) {
            throw new FileNotFoundException(
                    String.format("No file for perma %s found in %s", name, dir));
        }
        return fullFile;
    }

    boolean hasSameFullFileAs(FileGroup other) {
        return Objects.equals(fullFile, other.fullFile);
    }

    PermaFile latestDeltaFile() throws FileNotFoundException {
        if (deltaFiles.isEmpty()) {
            throw new FileNotFoundException(
                    String.format("No delta file for perma %s found in %s", name, dir));
        }
        return deltaFiles.get(deltaFiles.size() - 1);
    }

    List<PermaFile> deltaFiles() {
        return deltaFiles;
    }

    List<PermaFile> deltaFilesSince(FileGroup previousFiles) {
        if (previousFiles.deltaFiles.isEmpty()) {
            return deltaFiles;
        }
        return deltaFiles.subList(
                previousFiles.deltaFiles.size(),
                deltaFiles.size());
    }

    FileGroup withNextFull(Compression compression) {
        if (fullFile == null) {
            return new FileGroup(
                    dir,
                    name,
                    PermaFile.fullFile(compression, dir, name, 1),
                    ImmutableList.of());
        }
        return new FileGroup(
                dir,
                name,
                fullFile.nextFull(compression),
                ImmutableList.of());
    }

    FileGroup withNextDelta() {
        return new FileGroup(dir,
                name,
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

    boolean delete() throws IOException {
        if (!exists()) {
            return false;
        }
        boolean deleted = fullFile().delete();
        for (PermaFile deltaFile : deltaFiles()) {
            boolean deletedDelta = deltaFile.delete();
            deleted = deleted || deletedDelta;
        }
        return deleted;
    }

    File createTempFile() {
        return new File(dir, String.format(TEMP_FILE_FORMAT, name, UUID.randomUUID()));
    }

    void deleteStaleTempFiles() {
        Pattern tempFilePattern = Pattern.compile(String.format(TEMP_FILE_PATTERN_TEMPLATE, name));
        Arrays.stream(listDir(dir, (dir, s) -> tempFilePattern.matcher(s).matches()))
                .forEach(filename -> new File(dir, filename).delete());
    }

    Compression compression() throws FileNotFoundException {
        return fullFile.compression();
    }

    private File toFile(PermaFile fileName) {
        return new File(dir, fileName.toString());
    }

    private static List<PermaFile> deltaFileNamesOf(File dir, PermaFile fullFileName) {
        DeltaFilePattern deltaFilePattern = fullFileName.deltaFileNamePattern();
        return deltaFilePattern.parse(listDir(dir, deltaFilePattern));
    }

    private static Optional<PermaFile> latestFullFileName(File dir, String name) {
        FullFilePattern fullFilePattern = new FullFilePattern(name);
        return Arrays
                .stream(listDir(dir, fullFilePattern))
                .map(fileName -> fullFilePattern.parse(dir, fileName))
                .max(Comparator.naturalOrder());
    }

    private static String[] listDir(File dir, FilenameFilter filenameFilter) {
        String[] list = dir.list(filenameFilter);
        return list != null ? list : new String[]{};
    }

    @Override
    public String toString() {
        return "FileGroup{" +
                "dir=" + dir +
                ", name='" + name + '\'' +
                ", fullFile='" + fullFile + '\'' +
                ", deltaFiles=" + deltaFiles +
                '}';
    }
}
