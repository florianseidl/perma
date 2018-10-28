/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.serializers.KeyOrValueSerializer.*

class PermaSetTest extends SpecificationWithTempDir {
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)

    @Unroll
    def "write read #nr"() {
        given:
        def permaSet = WritablePermaSet.loadOrCreate(tempDir, "testset", serializer)

        when:
        permaSet.addAll(set)
        permaSet.persist();
        def permaSetReread = WritablePermaSet.loadOrCreate(tempDir, "testset", serializer)

        then:
        permaSetReread.equals(set)

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
        def permaSet = WritablePermaSet.loadOrCreateStringSet(tempDir, 'testset')

        when:
        permaSet.addAll(set)
        permaSet.persist();
        def permaSetReread = WritablePermaSet.loadOrCreateStringSet(tempDir, "testset")

        then:
        permaSetReread.equals(set)
    }

    def "write read readOnly string set"() {
        given:
        def set = ['foo', 'N I X', 'bla bla bla'] as Set
        def permaSet = WritablePermaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        permaSet.addAll(set)
        permaSet.persist();
        def permaReread = ReadOnlyPermaSet.loadStringSet(tempDir, "testset")

        then:
        permaReread.equals(set)
    }

    def "read readOnly string no files"() {
        when:
        def permaSetReread = ReadOnlyPermaSet.loadStringSet(tempDir, "testset")

        then:
        permaSetReread.equals([] as Set)
    }

    @Unroll
    def "write read write update readOnly string set #initial #update"() {
        given:
        def writablePermaSet = WritablePermaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        writablePermaSet.addAll(initial)
        writablePermaSet.persist()
        def readOnlyPermaSet = ReadOnlyPermaSet.loadStringSet(tempDir, "testset")
        writablePermaSet.clear()
        writablePermaSet.addAll(update)
        writablePermaSet.persist()
        readOnlyPermaSet.refresh()

        then:
        readOnlyPermaSet.equals(update)

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
        def permaSet = WritablePermaSet.loadOrCreateStringSet(tempDir, "testmap")

        when:
        [['foo'],
         ['foo', 'N I X'],
         ['foo', 'N I X', 'long store'],
         ['N I X', 'long store']].forEach {
            permaSet.persist()
            permaSet.clear()
            permaSet.addAll(it)
        }
        permaSet.compact()
        def permaRereadSet = ReadOnlyPermaSet.loadStringSet(tempDir, "testmap")

        then:
        permaSet.equals(['N I X','long store'] as Set)
        permaRereadSet.equals(['N I X','long store'] as Set)
    }
}