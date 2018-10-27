/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file

import ch.sbb.perma.SpecificationWithTempDir
import spock.lang.Unroll

class FileGroupTest extends SpecificationWithTempDir {

    @Unroll
    def "latest file #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def latest = FileGroup.list(tempDir, permaName)

        then:
        hasName(latest.fullFile(), latestFullFile)
        hasNames(latest.deltaFiles(), latestDeltaFiles)

        where:
        files                                                | permaName || latestFullFile  | latestDeltaFiles
        ['foo_1_0.perma']                                    | 'foo'     || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'foo_2_0.perma']                   | 'foo'     || 'foo_2_0.perma' | []
        ['foo_1_0.perma', 'foo_0_1.perma']                   | 'foo'     || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'bar_1_0.perma']                   | 'foo'     || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'bar_1_0.perma']                   | 'bar'     || 'bar_1_0.perma' | []
        ['foo_2_0.perma', 'foo_2_1.perma',
         'foo_1_0.perma', 'foo_1_1.perma', 'foo_1_2.perma']  | 'foo'     || 'foo_2_0.perma' | ['foo_2_1.perma']
        ['foo_1_0.perma', 'foo_1_1.perma',
         'foo_1_2.perma', 'foo_1_3.perma', 'foo_1_42.perma'] | 'foo'     || 'foo_1_0.perma' | ['foo_1_1.perma', 'foo_1_2.perma',
                                                                                               'foo_1_3.perma', 'foo_1_42.perma']
    }

    @Unroll
    def "no latest file #existingFiles"() {
        given:
        existingFiles.forEach {
            touch(it)
        }

        when:
        def files = FileGroup.list(tempDir, permaName)

        then:
        !files.exists()
        files.deltaFiles().isEmpty()

        where:
        existingFiles      | permaName
        []                 | 'foo'
        ['foo_1_0.perma']  | 'bar'
        ['foo_1_42.perma'] | 'foo'
    }


    @Unroll
    def "latest full file gzip #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def latest = FileGroup.list(tempDir, 'foo')

        then:
        hasName(latest.fullFile(), latestFullFile)
        hasNames(latest.deltaFiles(), latestDeltaFiles)

        where:
        files                                        || latestFullFile       | latestDeltaFiles
        ['foo_1_0.perma.gzip']                       || 'foo_1_0.perma.gzip' | []
        ['foo_1_0.perma.gzip', 'foo_2_0.perma.gzip'] || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma', 'foo_2_0.perma.gzip']      || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma.gzip', 'foo_2_0.perma']      || 'foo_2_0.perma'      | []
        ['foo_1_0.perma.gzip', 'foo_1_1.perma.gzip'] || 'foo_1_0.perma.gzip' | ['foo_1_1.perma.gzip']
        ['foo_1_0.perma.gzip', 'foo_1_1.perma.gzip',
         'foo_2_0.perma.gzip']                       || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma', 'foo_1_1.perma',
         'foo_2_0.perma.gzip', 'foo_2_1.perma.gzip'] || 'foo_2_0.perma.gzip' | ['foo_2_1.perma.gzip']
        ['foo_1_0.perma.gzip', 'foo_1_1.perma.gzip',
         'foo_1_2.perma.gzip']                       || 'foo_1_0.perma.gzip' | ['foo_1_1.perma.gzip', 'foo_1_2.perma.gzip']

    }

    @Unroll
    def "nextDeltaFile #files"() {
        given:
        files.forEach {
            touch(it)
        }
        FileGroup fileGroup = FileGroup.list(tempDir, 'foo')

        when:
        def nextDeltaFile = fileGroup
                .withNextDelta()
                .latestDeltaFile()

        then:
        hasName(nextDeltaFile, expectedNextDeltaFile)

        where:
        files                               || expectedNextDeltaFile
        ['foo_1_0.perma']                   || 'foo_1_1.perma'
        ['foo_1_0.perma', 'foo_2_0.perma']  || 'foo_2_1.perma'
        ['foo_1_0.perma', 'foo_1_43.perma'] || 'foo_1_44.perma'
        ['foo_1_0.perma.gzip']              || 'foo_1_1.perma.gzip'
    }

    @Unroll
    def "nextFullFile #files #compression.class.simplepermaName"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def nextFullFile = FileGroup
                .list(tempDir, 'foo')
                .withNextFull(compression)
                .fullFile()

        then:
        hasName(nextFullFile, expectedNextFullFile)

        where:
        files                               | compression           || expectedNextFullFile
        ['foo_1_0.perma']                   | new NoCompression()   || 'foo_2_0.perma'
        ['foo_1_0.perma', 'foo_7_0.perma']  | new NoCompression()   || 'foo_8_0.perma'
        ['foo_1_0.perma', 'foo_1_43.perma'] | new NoCompression()   || 'foo_2_0.perma'
        []                                  | new NoCompression()   || 'foo_1_0.perma'
        ['foo_1_43.perma']                  | new NoCompression()   || 'foo_1_0.perma'
        ['foo_1_0.perma.gzip']              | new GZipCompression() || 'foo_2_0.perma.gzip'
        ['foo_1_0.perma', 'foo_2_0.perma']  | new GZipCompression() || 'foo_3_0.perma.gzip'
        []                                  | new GZipCompression() || 'foo_1_0.perma.gzip'
    }

    def touch(permaName) {
        new File(tempDir, permaName).createNewFile();
    }

    def "list no file FileNotFoundException"() {
        when:
        FileGroup.list(tempDir, 'gibtsned').fullFile()

        then:
        thrown FileNotFoundException
    }

    @Unroll
    def "latest delta for #existingFiles FileNotFoundExceptoin"() {
        given:
        existingFiles.forEach {
            touch(it)
        }

        when:
        FileGroup.list(tempDir, 'foo').latestDeltaFile()

        then:
        thrown FileNotFoundException

        where:
        existingFiles << [[], ['foo_1_0.perma'], ['foo_1_42.perma']]
    }

    @Unroll
    def "delta files of #filesToWriteNow since #filesToWriteBefore"() {
        given:
        filesToWriteBefore.forEach {
            touch(it)
        }
        def filesBefore = FileGroup.list(tempDir, 'foo')

        when:
        filesToWriteNow.forEach {
            touch(it)
        }
        def filesNow = filesBefore.refresh();
        def deltaFilesSince = filesNow.deltaFilesSince(filesBefore)

        then:
        hasNames(deltaFilesSince, expectedDeltaFilesSince)

        where:
        filesToWriteBefore                 | filesToWriteNow                                     || expectedDeltaFilesSince
        ['foo_1_0.perma']                  | ['foo_1_0.perma', 'foo_1_1.perma']                  || ['foo_1_1.perma']
        []                                 | ['foo_1_0.perma', 'foo_1_1.perma']                  || ['foo_1_1.perma']
        []                                 | ['foo_1_0.perma']                                   || []
        ['foo_1_0.perma', 'foo_1_1.perma'] | ['foo_1_0.perma', 'foo_1_1.perma', 'foo_1_2.perma'] || ['foo_1_2.perma']
        ['foo_1_0.perma', 'foo_1_1.perma'] | ['foo_1_0.perma', 'foo_1_1.perma', 'foo_1_2.perma',
                                              'foo_1_3.perma']                                   || ['foo_1_2.perma', 'foo_1_3.perma']
        ['foo_1_0.perma', 'foo_1_1.perma'] | ['foo_1_0.perma', 'foo_1_1.perma', 'foo_1_3.perma'] || ['foo_1_3.perma']
    }

    @Unroll
    def "delete #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        FileGroup
                .list(tempDir, 'foo')
                .delete()

        then:
        tempDir.list() == remaining as String[]

        where:
        files                              || remaining
        ['foo_1_0.perma']                  || []
        ['foo_1_0.perma', 'foo_2_0.perma'] || ['foo_1_0.perma']
        ['foo_1_0.perma', 'foo_1_1.perma',
         'foo_2_0.perma', 'foo_2_1.perma'] || ['foo_1_0.perma', 'foo_1_1.perma']
        []                                 || []
    }

    @Unroll
    def "hasSameFullFileAs #filesA #filesB"() {
        given:
        filesA.forEach {
            touch(it)
        }

        when:
        def filesBeforeB = FileGroup.list(tempDir, 'foo')
        filesB.forEach {
            touch(it)
        }
        def filesAfterB = FileGroup.list(tempDir, 'foo')
        def sameFullFile = filesAfterB.hasSameFullFileAs(filesBeforeB)

        then:
        sameFullFile == expectSameFullFile

        where:
        filesA                             | filesB                             || expectSameFullFile
        ['foo_1_0.perma']                  | ['foo_1_0.perma']                  || true
        ['foo_1_0.perma']                  | ['foo_1_0.perma', 'foo_1_1.perma'] || true
        ['foo_1_0.perma', 'foo_1_1.perma'] | ['foo_1_0.perma']                  || true
        ['foo_1_0.perma']                  | ['foo_2_0.perma']                  || false
        []                                 | ['foo_1_0.perma']                  || false
        []                                 | []                                 || true
    }

    @Unroll
    def "compression #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def compression = FileGroup.list(tempDir, 'foo').fullFile().compression()

        then:
        compression.class == expectedCompression

        where:
        files                                   || expectedCompression
        ['foo_1_0.perma']                       || NoCompression.class
        ['foo_1_0.perma.gzip']                  || GZipCompression.class
        ['foo_1_0.perma', 'foo_2_0.perma']      || NoCompression.class
        ['foo_1_0.perma', 'foo_2_0.perma.gzip'] || GZipCompression.class
        ['foo_1_0.perma.gzip', 'foo_2_0.perma'] || NoCompression.class
        ['foo_1_0.perma', 'foo_1_1.perma',
         'foo_2_0.perma.gzip']                  || GZipCompression.class
        ['foo_1_0.perma', 'foo_1_1.perma.gzip'] || NoCompression.class // invalid szenario
    }

    def toStringIsImplemented() {
        when:
        def fileGroupToString = FileGroup.list(tempDir, 'foo').toString()

        then:
        !fileGroupToString.contains('@')
    }

    private static boolean hasName(PermaFile file, String name) {
        return nameOf(file).equals(name)
    }

    private static boolean hasNames(List<PermaFile> files, List<String> names) {
        return files.collect{nameOf(it)}.equals(names)
    }

    private static String nameOf(def permaOrTempFile) {
        return new File(permaOrTempFile.toString()).name
    }
}
