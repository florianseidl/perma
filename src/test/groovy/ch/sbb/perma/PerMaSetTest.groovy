/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.serializers.KeyOrValueSerializer.*

class PerMaSetTest extends Specification {
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)
    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def cleanup() {
        tempDir.deleteDir()
    }

    @Unroll
    def "write read #nr"() {
        given:
        def perMaSet = WritablePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        when:
        perMaSet.addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritablePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        then:
        perMaSetReread.equals(set)

        where:
        nr | set                                   | serializer
        1  | ['foo'] as Set                        | STRING
        2  | [] as Set                             | STRING
        3  | ['foo', 'b A r', LONG_STRING] as Set  | STRING
        4  | [7,42,9999,66666] as Set              | INTEGER
        5  | [999999999999L] as Set                | LONG
        6  | [LocalDate.MAX, LocalDate.MIN] as Set | JAVA_OBJECT
    }

    def "write read string set"() {
        given:
        def set = ['foo', 'bar'] as Set
        def perMaSet = WritablePerMaSet.loadOrCreateStringSet(tempDir, 'testset')

        when:
        perMaSet.addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritablePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        then:
        perMaSetReread.equals(set)
    }

    def "write read readOnly string set"() {
        given:
        def set = ['foo', 'N I X', 'bla bla bla'] as Set
        def perMaSet = WritablePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        perMaSet.addAll(set)
        perMaSet.persist();
        def perMaReread = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")

        then:
        perMaReread.equals(set)
    }

    def "read readOnly string no files"() {
        when:
        def perMaSetReread = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")

        then:
        perMaSetReread.equals([] as Set)
    }

    @Unroll
    def "write read write update readOnly string set #initial #update"() {
        given:
        def writablePerMaSet = WritablePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        writablePerMaSet.addAll(initial)
        writablePerMaSet.persist()
        def readOnlyPerMaSet = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")
        writablePerMaSet.clear()
        writablePerMaSet.addAll(update)
        writablePerMaSet.persist()
        readOnlyPerMaSet.refresh()

        then:
        readOnlyPerMaSet.equals(update)

        where:
        initial                                     | update
        ['foo'] as Set                              | ['foo', 'N I X'] as Set
        ['foo', 'N I X'] as Set                     | ['foo', 'N I X'] as Set
        ['foo', 'N I X', 'nix als bledsinn'] as Set | [] as Set
        ['foo', 'N I X', 'nix als bledsinn'] as Set | ['foo', 'nix als bledsinn', 'bli bla blo'] as Set
        [] as Set                                   | [] as Set
        [] as Set                                   | ['foo', 'N I X', 'nix als bledsinn'] as Set
    }

    def "write compact reread string set"() {
        given:
        def perMaSet = WritablePerMaSet.loadOrCreateStringSet(tempDir, "testmap")

        when:
        [['foo'],
         ['foo', 'N I X'],
         ['foo', 'N I X', 'long store'],
         ['N I X', 'long store']].forEach {
            perMaSet.persist()
            perMaSet.clear()
            perMaSet.addAll(it)
        }
        perMaSet.compact()
        def perMaRereadSet = ReadOnlyPerMaSet.loadStringSet(tempDir, "testmap")

        then:
        perMaSet.equals(['N I X','long store'] as Set)
        perMaRereadSet.equals(['N I X','long store'] as Set)
    }
}