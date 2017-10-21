/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PerMa files in a directory. Can List, create new files,...
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class Directory {
    class Listing {
        private final String fullFileName;
        private final List<String> deltaFileNames;

        private Listing(String fullFileName) {
            this.fullFileName = fullFileName;
            this.deltaFileNames = Directory.this.deltaFileNamesOf(fullFileName);
        }

        public File fullFile() {
            return Directory.this.file(fullFileName);
        }

        public List<File> deltaFiles() {
            return deltaFileNames.stream()
                    .map(Directory.this::file)
                    .collect(Collectors.toList());
        }

        private int lastestDeltaFileNumber() {
            if(deltaFileNames.isEmpty()) {
                return 0;
            }
            return Directory.this.deltaFileNumber(fullFileName, deltaFileNames.get(deltaFileNames.size() -1));
        }

        public File nextDeltaFile() {
            return Directory.this.file(
                      String.format(FILE_FORMAT,
                                    name,
                                    Directory.this.fullFileNumber(fullFileName),
                                    lastestDeltaFileNumber() + 1));
        }
    }
    private final static String FILE_FORMAT = "%s_%d_%d.perma";
    private final static String FULL_FILE_NAME_PATTERN_TEMPLATE = "%s_(\\d+)_0\\.perma";
    private final static String DELTA_FILE_NAME_PATTERN_TEMPLATE = "%s_%d_([1-9]\\d*)\\.perma";
    private final static String TEMP_FILE_FORMAT = "%s_%s.temp";

    private final File dir;
    private final String  name;
    private final Pattern fullFilePattern;

    public Directory(File dir, String name) {
        this.dir = dir;
        this.name = name;
        this.fullFilePattern = Pattern.compile(
                String.format(FULL_FILE_NAME_PATTERN_TEMPLATE, name));
    }

    public boolean fileExists() {
        return latestFullFileName().isPresent();
    }

    public Listing listLatest() throws FileNotFoundException {
        return latestFullFileName()
                .map(Listing::new)
                .orElseThrow(() -> new FileNotFoundException(
                        String.format("No file for %s found in %s",name, dir)));
    }


    public File nextFullFile() {
        int latestFullFileNumber = latestFullFileName().map(this::fullFileNumber).orElse(0);
        return new File(dir, String.format(FILE_FORMAT, name, latestFullFileNumber + 1, 0));
    }

    private File file(String fileName) {
        return new File(dir, fileName);
    }

    private List<String> deltaFileNamesOf(String fullFileName) {
        Pattern deltaFilePattern = deltaFilePatttern(fullFileName);
        return Arrays.stream(listFileNames(deltaFilePattern))
                .sorted(Comparator.comparingInt(fileName -> deltaFileNumber(fullFileName, fileName)))
                .collect(Collectors.toList());
    }

    private Optional<String> latestFullFileName() {
        return Arrays.stream(listFileNames(fullFilePattern))
                .sorted(Comparator.comparingInt(this::fullFileNumber).reversed())
                .findFirst();
    }

    private String[] listFileNames(Pattern pattern) {
        return dir.list(
                (file, s) -> pattern.matcher(s).matches()
        );
    }


    private int fullFileNumber(String fileName) {
        return parseFileNumber(fullFilePattern, fileName);
    }

    private int deltaFileNumber(String fullFileName, String fileName) {
        return parseFileNumber(deltaFilePatttern(fullFileName), fileName);
    }

    private Pattern deltaFilePatttern(String fullFileName) {
        int fullFileNumber = fullFileNumber(fullFileName);
        return Pattern.compile(String.format(DELTA_FILE_NAME_PATTERN_TEMPLATE, name, fullFileNumber));
    }

    private int parseFileNumber(Pattern pattern, String fileName) {
        if(fileName == null) {
            return 0;
        }
        Matcher matcher = pattern.matcher(fileName);
        matcher.find();
        return Integer.parseInt(matcher.group(1));
    }

    File tempFile() {
        return new File(dir, String.format(TEMP_FILE_FORMAT, name, UUID.randomUUID()));
    }

}
