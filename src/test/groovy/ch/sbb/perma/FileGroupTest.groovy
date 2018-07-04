/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import ch.sbb.perma.datastore.GZipCompression
import ch.sbb.perma.datastore.NoCompression
import spock.lang.Specification
import spock.lang.Unroll

class FileGroupTest extends Specification {
    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def cleanup() {
        tempDir.deleteDir()
    }

    @Unroll
    def "latest file #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def latest = FileGroup.list(tempDir, name)

        then:
        latest.fullFile().name == latestFullFile
        latest.deltaFiles().collect { it.name } == latestDeltaFiles

        where:
        files                                              | name  || latestFullFile  | latestDeltaFiles
        ['foo_1_0.perma']                                  | 'foo' || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'foo_2_0.perma']                 | 'foo' || 'foo_2_0.perma' | []
        ['foo_1_0.perma', 'foo_0_1.perma']                 | 'foo' || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'bar_1_0.perma']                 | 'foo' || 'foo_1_0.perma' | []
        ['foo_1_0.perma', 'bar_1_0.perma']                 | 'bar' || 'bar_1_0.perma' | []
        ['foo_2_0.perma', 'foo_2_1.perma',
         'foo_1_0.perma','foo_1_1.perma','foo_1_2.perma']  | 'foo' || 'foo_2_0.perma' | ['foo_2_1.perma']
        ['foo_1_0.perma', 'foo_1_1.perma',
         'foo_1_2.perma','foo_1_3.perma','foo_1_42.perma'] | 'foo' || 'foo_1_0.perma' | ['foo_1_1.perma', 'foo_1_2.perma',
                                                                                         'foo_1_3.perma','foo_1_42.perma']
    }

    @Unroll
    def "no latest file #existingFiles"() {
        given:
        existingFiles.forEach {
            touch(it)
        }

        when:
        def files = FileGroup.list(tempDir, name)

        then:
        !files.exists()
        files.deltaFiles().isEmpty()

        where:
        existingFiles      | name
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
        latest.fullFile().name == latestFullFile
        latest.deltaFiles().collect { it.name } == latestDeltaFiles

        where:
        files                                       || latestFullFile       | latestDeltaFiles
        ['foo_1_0.perma.gzip']                      || 'foo_1_0.perma.gzip' | []
        ['foo_1_0.perma.gzip','foo_2_0.perma.gzip'] || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma','foo_2_0.perma.gzip']      || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma.gzip','foo_2_0.perma']      || 'foo_2_0.perma'      | []
        ['foo_1_0.perma.gzip','foo_1_1.perma.gzip'] || 'foo_1_0.perma.gzip' | ['foo_1_1.perma.gzip']
        ['foo_1_0.perma.gzip','foo_1_1.perma.gzip',
         'foo_2_0.perma.gzip']                      || 'foo_2_0.perma.gzip' | []
        ['foo_1_0.perma','foo_1_1.perma',
         'foo_2_0.perma.gzip','foo_2_1.perma.gzip'] || 'foo_2_0.perma.gzip' | ['foo_2_1.perma.gzip']
        ['foo_1_0.perma.gzip','foo_1_1.perma.gzip',
         'foo_1_2.perma.gzip']                      || 'foo_1_0.perma.gzip' | ['foo_1_1.perma.gzip','foo_1_2.perma.gzip']

    }

    @Unroll
    def "nextDeltaFile #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def nextDeltaFile = FileGroup
                .list(tempDir, 'foo' )
                .withNextDelta()
                .latestDeltaFile()

        then:
        nextDeltaFile.name == nextDeltaFileName

        where:
        files                              || nextDeltaFileName
        ['foo_1_0.perma']                  || 'foo_1_1.perma'
        ['foo_1_0.perma', 'foo_2_0.perma'] || 'foo_2_1.perma'
        ['foo_1_0.perma','foo_1_43.perma'] || 'foo_1_44.perma'
        ['foo_1_0.perma.gzip']             || 'foo_1_1.perma.gzip'
    }

    @Unroll
    def "nextFullFile #files #compression.class.simpleName"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def nextFullFile = FileGroup
                .list(tempDir, 'foo' )
                .withNextFull(compression)
                .fullFile()

        then:
        nextFullFile.name == nextFullFileName

        where:
        files                              | compression           || nextFullFileName
        ['foo_1_0.perma']                  | new NoCompression()   || 'foo_2_0.perma'
        ['foo_1_0.perma', 'foo_7_0.perma'] | new NoCompression()   || 'foo_8_0.perma'
        ['foo_1_0.perma','foo_1_43.perma'] | new NoCompression()   || 'foo_2_0.perma'
        []                                 | new NoCompression()   || 'foo_1_0.perma'
        ['foo_1_43.perma']                 | new NoCompression()   || 'foo_1_0.perma'
        ['foo_1_0.perma.gzip']             | new GZipCompression() || 'foo_2_0.perma.gzip'
        ['foo_1_0.perma','foo_2_0.perma']  | new GZipCompression() || 'foo_3_0.perma.gzip'
        []                                 | new GZipCompression() || 'foo_1_0.perma.gzip'
    }

    def touch(name) {
        new File(tempDir, name).createNewFile();
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
        existingFiles << [[], ['foo_1_0.perma'],['foo_1_42.perma']]
    }

    @Unroll
    def "delta files of #filesToWriteNow since #filesToWriteBefore"() {
        given:
        filesToWriteBefore.forEach {
            touch(it)
        }

        when:
        def filesBefore = FileGroup.list(tempDir, 'foo' )
        filesToWriteNow.forEach {
            touch(it)
        }
        def filesNow = filesBefore.refresh();
        def deltaFilesSince = filesNow.deltaFilesSince(filesBefore)

        then:
        deltaFilesSince.collect { it.name } == result

        where:
        filesToWriteBefore                 | filesToWriteNow                                   || result
        ['foo_1_0.perma']                  | ['foo_1_0.perma','foo_1_1.perma']                 || ['foo_1_1.perma']
        []                                 | ['foo_1_0.perma','foo_1_1.perma']                 || ['foo_1_1.perma']
        []                                 | ['foo_1_0.perma']                                 || []
        ['foo_1_0.perma','foo_1_1.perma']  | ['foo_1_0.perma','foo_1_1.perma','foo_1_2.perma'] || ['foo_1_2.perma']
        ['foo_1_0.perma','foo_1_1.perma']  | ['foo_1_0.perma','foo_1_1.perma','foo_1_2.perma',
                                              'foo_1_3.perma']                                 || ['foo_1_2.perma','foo_1_3.perma']
        ['foo_1_0.perma','foo_1_1.perma']  | ['foo_1_0.perma','foo_1_1.perma','foo_1_3.perma'] || ['foo_1_3.perma']
    }

    @Unroll
    def "delete #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        FileGroup
                .list(tempDir, 'foo' )
                .delete()

        then:
        tempDir.list() == remaining

        where:
        files                              || remaining
        ['foo_1_0.perma']                  || []
        ['foo_1_0.perma', 'foo_2_0.perma'] || ['foo_1_0.perma']
        ['foo_1_0.perma','foo_1_1.perma',
         'foo_2_0.perma','foo_2_1.perma']  || ['foo_1_0.perma','foo_1_1.perma']
        []                                 || []
    }

    @Unroll
    def "hasSameFullFileAs #filesA #filesB"() {
        given:
        filesA.forEach {
            touch(it)
        }

        when:
        def filesBeforeB = FileGroup.list(tempDir, 'foo' )
        filesB.forEach {
            touch(it)
        }
        def filesAfterB = FileGroup.list(tempDir, 'foo' )
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
        def compression = FileGroup.list(tempDir, 'foo' ).compression()

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



    def "create temp file"() {
        when:
        def tempFile = FileGroup
                .list(tempDir, 'foo' )
                .createTempFile()

        then:
        tempFile.getName().startsWith('foo')
        tempFile.getName().endsWith('perma.temp')
    }


    def "delete old temp file"() {
        given:
        def fileGroup = FileGroup
                .list(tempDir, 'foo' )
        def staleTempFile = fileGroup.createTempFile()
        staleTempFile.write('irgendwas bli bla blo')

        when:
        fileGroup.deleteStaleTempFiles()

        then:
        !staleTempFile.exists()
    }

    def toStringIsImplemented() {
        when:
        def fileGroupToString = FileGroup.list(tempDir, 'foo').toString()

        then:
        !fileGroupToString.contains('@')
    }
}
