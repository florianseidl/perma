/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import ch.sbb.perma.datastore.Compression;
import ch.sbb.perma.datastore.GZipCompression;
import ch.sbb.perma.datastore.NoCompression;
import ch.sbb.perma.file.FileName;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
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
            if (!matcher.find()) {
                throw new IllegalArgumentException(String.format("Invalid file name %s", fileName));
            }
            return Integer.parseInt(matcher.group(1));
        }

        String[] listFileNames(File dir) {
            return listDir(dir, pattern);
        }
    }

    private final static String TEMP_FILE_FORMAT = "%s-%s.perma.temp";
    private final static String FULL_FILE_NAME_PATTERN_TEMPLATE = "%s_(\\d+)_0\\.perma(\\.gzip)?";
    private final static String DELTA_FILE_NAME_PATTERN_TEMPLATE = "%s_%d_([1-9]\\d*)\\.perma(\\.gzip)?";
    private final static Pattern GZIP_FILE_NAME_PATTERN = Pattern.compile(".*\\.perma\\.gzip");
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

    static FileGroup list(File dir, String name) {
        return latestFullFileName(dir, name)
                .map(latestFullFileName -> new FileGroup(dir,
                        name,
                        latestFullFileName,
                        deltaFileNamesOf(dir, name, latestFullFileName)))
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
                    compression.fileNameFormat().fullFile(name, 1),
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
        Arrays.stream(listDir(dir, tempFilePattern))
                .forEach(filename -> new File(dir, filename).delete());
    }

    Compression compression() throws FileNotFoundException {
        return fullFileName.compression();
    }

    private Compression compressionForExistingGroup() {
        try {
            return compression();
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("File should exist to dermine compression but was not found", e);
        }
    }

    private File toFile(FileName fileName) {
        return new File(dir, fileName.toString());
    }

    private static ImmutableList<FileName> deltaFileNamesOf(File dir, String name, FileName fullFileName) {
        FilePattern deltaFilePattern = deltaFilePattern(name, fullFileName.toString());
        return Arrays.stream(deltaFilePattern.listFileNames(dir))
                .map(fileName -> fullFileName.delta(deltaFilePattern.parseFileNumber(fileName)))
                .sorted(Comparator.naturalOrder())
                .collect(ImmutableList.toImmutableList());
    }

    private static Optional<FileName> latestFullFileName(File dir, String name) {
        FilePattern fullFilePattern = fullFilePattern(name);
        return Arrays
                .stream(fullFilePattern.listFileNames(dir))
                .map(fileName -> compressionOf(fileName).fileNameFormat().fullFile(name, fullFilePattern(name).parseFileNumber(fileName)))
                .max(Comparator.naturalOrder());
    }

    private static Compression compressionOf(String fileName) {
        if (GZIP_FILE_NAME_PATTERN.matcher(fileName).matches()) {
            return new GZipCompression();
        }
        return new NoCompression();
    }


    private static FilePattern fullFilePattern(String name) {
        return new FilePattern(FULL_FILE_NAME_PATTERN_TEMPLATE, name);
    }

    private static FilePattern deltaFilePattern(String name, String fullFileName) {
        int fullFileNumber = fullFilePattern(name).parseFileNumber(fullFileName);
        return new FilePattern(String.format(DELTA_FILE_NAME_PATTERN_TEMPLATE, name, fullFileNumber));
    }

    private static String[] listDir(File dir, Pattern pattern) {
        String[] list = dir.list(
                (file, s) -> pattern.matcher(s).matches()
        );
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
