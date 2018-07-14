/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.datastore.GZipCompression;
import ch.sbb.perma.datastore.NoCompression;
import ch.sbb.perma.file.DeltaFilePattern;
import ch.sbb.perma.file.FileName;
import ch.sbb.perma.file.FullFilePattern;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Writable files in a directory. Can List, create new files,...
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class FileGroup {

    private static class FilePattern {
        private final Pattern pattern;

        FilePattern(String template, Object... params) {
            this.pattern = Pattern.compile(String.format(template, params));
        }

        int parseFileNumber(String fileName) {
            if (fileName == null) {
                return 0;
            }
            Matcher matcher = pattern.matcher(fileName);
            Preconditions.checkArgument(
                    matcher.find(),
                    String.format("Invalid file name %s", fileName));
            return Integer.parseInt(matcher.group(1));
        }

        String[] listFileNames(File dir) {
            return listDir(dir, pattern);
        }
    }

    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String TEMP_FILE_PATTERN_TEMPLATE = String.format(TEMP_FILE_FORMAT, "%s", ".+");

    private final File dir;
    private final String name;
    private final FileName fullFileName;
    private final ImmutableList<FileName> deltaFileNames;


    private FileGroup(File dir, String name, FileName fullFileName, ImmutableList<FileName> deltaFileNames) {
        this.dir = dir;
        this.name = name;
        this.fullFileName = fullFileName;
        this.deltaFileNames = deltaFileNames;
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
        return fullFileName != null;
    }

    File fullFile() throws FileNotFoundException {
        if (fullFileName == null) {
            throw new FileNotFoundException(
                    String.format("No file for perma %s found in %s", name, dir));
        }
        return toFile(fullFileName);
    }

    boolean hasSameFullFileAs(FileGroup other) {
        return Objects.equals(fullFileName, other.fullFileName);
    }

    File latestDeltaFile() throws FileNotFoundException {
        if (deltaFileNames.isEmpty()) {
            throw new FileNotFoundException(
                    String.format("No delta file for perma %s found in %s", name, dir));
        }
        return toFile(deltaFileNames.get(deltaFileNames.size() - 1));
    }

    List<File> deltaFiles() {
        return deltaFileNames.stream()
                .map(this::toFile)
                .collect(Collectors.toList());
    }


    List<File> deltaFilesSince(FileGroup previousFiles) {
        return deltaFileNamesSince(previousFiles)
                .stream()
                .map(this::toFile)
                .collect(Collectors.toList());
    }

    private List<FileName> deltaFileNamesSince(FileGroup previousFiles) {
        if (previousFiles.deltaFileNames.isEmpty()) {
            return deltaFileNames;
        }
        return deltaFileNames.subList(
                previousFiles.deltaFileNames.size(),
                deltaFileNames.size());
    }

    FileGroup withNextFull(Compression compression) {
        if (fullFileName == null) {
            return new FileGroup(
                    dir,
                    name,
                    FileName.fullFile(compression, name, 1),
                    ImmutableList.of());
        }
        return new FileGroup(
                dir,
                name,
                fullFileName.nextFull(compression),
                ImmutableList.of());
    }

    FileGroup withNextDelta() {
        return new FileGroup(dir,
                name,
                fullFileName,
                ImmutableList.<FileName>builder()
                        .addAll(deltaFileNames)
                        .add(nextDeltaFileName())
                        .build());
    }

    private FileName nextDeltaFileName() {
        if(deltaFileNames.isEmpty()) {
            return fullFileName.nextDelta();
        }
        return deltaFileNames.get(deltaFileNames.size() -1).nextDelta();
    }

    boolean delete() throws IOException {
        if (!exists()) {
            return false;
        }
        boolean deleted = fullFile().delete();
        for (File deltaFile : deltaFiles()) {
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
        return fullFileName.compression();
    }

    private File toFile(FileName fileName) {
        return new File(dir, fileName.toString());
    }

    private static List<FileName> deltaFileNamesOf(File dir, FileName fullFileName) {
        DeltaFilePattern deltaFilePattern = fullFileName.deltaFileNamePattern();
        return deltaFilePattern.parse(listDir(dir, deltaFilePattern));
    }

    private static Optional<FileName> latestFullFileName(File dir, String name) {
        FullFilePattern fullFilePattern = new FullFilePattern(name);
        return Arrays
                .stream(listDir(dir, fullFilePattern))
                .map(fullFilePattern::parse)
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
                ", fullFileName='" + fullFileName + '\'' +
                ", deltaFileNames=" + deltaFileNames +
                '}';
    }
}
