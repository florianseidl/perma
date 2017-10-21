/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Specification
import spock.lang.Unroll

class DirectoryTest extends Specification {
    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    @Unroll
    def "latest file #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def latest = new Directory(tempDir, name).listLatest()

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
    def "no latest file #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def exists = new Directory(tempDir, name).fileExists()

        then:
        !exists

        where:
        files              | name
        []                 | 'foo'
        ['foo_1_0.perma']  | 'bar'
        ['foo_1_42.perma'] | 'foo'
    }

    @Unroll
    def "nextDeltaFile #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def nextDeltaFile = new Directory(tempDir, 'foo' )
                .listLatest()
                .nextDeltaFile()

        then:
        nextDeltaFile.name == nextDeltaFileName

        where:
        files                              || nextDeltaFileName
        ['foo_1_0.perma']                  || 'foo_1_1.perma'
        ['foo_1_0.perma', 'foo_2_0.perma'] || 'foo_2_1.perma'
        ['foo_1_0.perma','foo_1_43.perma'] || 'foo_1_44.perma'
    }

    @Unroll
    def "nextFullFile #files"() {
        given:
        files.forEach {
            touch(it)
        }

        when:
        def nextFullFile = new Directory(tempDir, 'foo' ).nextFullFile()

        then:
        nextFullFile.name == nextFullFileName

        where:
        files                              || nextFullFileName
        ['foo_1_0.perma']                  || 'foo_2_0.perma'
        ['foo_1_0.perma', 'foo_7_0.perma'] || 'foo_8_0.perma'
        ['foo_1_0.perma','foo_1_43.perma'] || 'foo_2_0.perma'
        []                                 || 'foo_1_0.perma'
        ['foo_1_43.perma']                 || 'foo_1_0.perma'
    }

    def touch(name) {
        new File(tempDir, name).createNewFile();
    }
}
