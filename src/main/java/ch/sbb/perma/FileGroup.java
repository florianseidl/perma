/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Writeable files in a directory. Can List, create new files,...
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class FileGroup {

    private static class FilePattern {
        private final Pattern pattern;

        public FilePattern(String template, Object... params) {
            this.pattern = Pattern.compile(String.format(template, params));
        }

        int parseFileNumber(String fileName) {
            if(fileName == null) {
                return 0;
            }
            Matcher matcher = pattern.matcher(fileName);
            matcher.find();
            return Integer.parseInt(matcher.group(1));
        }


        String[] listFileNames(File dir) {
            return dir.list(
                    (file, s) -> pattern.matcher(s).matches()
            );
        }

    }

    private final static String FULL_FILE_NAME_PATTERN_TEMPLATE = "%s_(\\d+)_0\\.perma";
    private final static String DELTA_FILE_NAME_PATTERN_TEMPLATE = "%s_%d_([1-9]\\d*)\\.perma";
    private final static String FILE_FORMAT = "%s_%d_%d.perma";

    private final File dir;
    private final String name;
    private final String fullFileName;
    private final ImmutableList<String> deltaFileNames;


    private FileGroup(File dir, String name, String fullFileName, ImmutableList<String> deltaFileNames) {
        this.dir = dir;
        this.name = name;
        this.fullFileName = fullFileName;
        this.deltaFileNames = deltaFileNames;
    }

    static FileGroup list(File dir, String name) throws FileNotFoundException {
        return latestFullFileName(dir, name)
                .map(latestFullFileName -> new FileGroup(dir,
                                                            name,
                                                            latestFullFileName,
                                                            deltaFileNamesOf(dir, name, latestFullFileName)))
                .orElse(new FileGroup(dir, name, null, ImmutableList.of()));
    }

    FileGroup refresh() throws FileNotFoundException {
        return list(dir, name);
    }

    boolean exists() {
        return fullFileName != null;
    }

    File fullFile() throws FileNotFoundException {
        if(fullFileName == null) {
            throw new FileNotFoundException(
                        String.format("No file for perma %s found in %s", name, dir));
        }
        return toFile(fullFileName);
    }

    boolean hasSameFullFileAs(FileGroup other) {
        return Objects.equals(fullFileName, other.fullFileName);
    }

    File latestDeltaFile() throws FileNotFoundException {
        if(deltaFileNames.isEmpty()) {
            throw new FileNotFoundException(
                        String.format("No delta file for perma %s found in %s", name, dir));
        }
        return toFile(deltaFileNames.get(deltaFileNames.size() -1));
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

    private List<String> deltaFileNamesSince(FileGroup previousFiles) {
        if(previousFiles.deltaFileNames.isEmpty()) {
            return deltaFileNames;
        }
        return deltaFileNames.subList(
                previousFiles.deltaFileNames.size(),
                deltaFileNames.size());
    }

    FileGroup withNextFull() {
        int latestFullFileNumber = fullFileName != null ?
                        fullFilePattern(name).parseFileNumber(fullFileName) : 0;
        return new FileGroup(
                        dir,
                        name,
                        String.format(FILE_FORMAT, name, latestFullFileNumber + 1, 0),
                        ImmutableList.of());
    }

    FileGroup withNextDelta() {
        int latestFullFileNumber = fullFilePattern(name).parseFileNumber(fullFileName);
        int latestDeltaFileNumber = !deltaFileNames.isEmpty() ? deltaFilePattern(name, fullFileName)
                                        .parseFileNumber(deltaFileNames.get(deltaFileNames.size() - 1))
                                        : 0;
        return new FileGroup(dir,
                                name,
                                fullFileName,
                                ImmutableList.<String>builder()
                                            .addAll(deltaFileNames)
                                            .add(String.format(FILE_FORMAT, name, latestFullFileNumber, latestDeltaFileNumber + 1))
                                            .build());
    }

    boolean delete() throws IOException {
        if(!exists()) {
            return false;
        }
        boolean deleted = fullFile().delete();
        for(File deltaFile : deltaFiles()) {
            boolean deletedDelta = deltaFile.delete();
            deleted = deleted || deletedDelta;
        }
        return deleted;
    }

    private File toFile(String fileName) {
        return new File(dir, fileName);
    }

    private static ImmutableList<String> deltaFileNamesOf(File dir, String name, String fullFileName) {
        FilePattern deltaFilePattern = deltaFilePattern(name, fullFileName);
        return Arrays.stream(deltaFilePattern.listFileNames(dir))
                .sorted(Comparator.comparingInt(deltaFilePattern::parseFileNumber))
                .collect(ImmutableList.toImmutableList());
    }

    private static Optional<String> latestFullFileName(File dir, String name) {
        FilePattern fullFilePattern = fullFilePattern(name);
        return Arrays.stream(fullFilePattern.listFileNames(dir))
                .sorted(Comparator.comparingInt(fullFilePattern::parseFileNumber).reversed())
                .findFirst();
    }

    private static FilePattern fullFilePattern(String name) {
        return new FilePattern(FULL_FILE_NAME_PATTERN_TEMPLATE, name);
    }

    private static FilePattern deltaFilePattern(String name, String fullFileName) {
        int fullFileNumber = fullFilePattern(name).parseFileNumber(fullFileName);
        return new FilePattern(String.format(DELTA_FILE_NAME_PATTERN_TEMPLATE, name, fullFileNumber));
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
