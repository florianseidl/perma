/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.datastore.KeyOrValueSerializer.*

class PerMaSetTest extends Specification {
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)
    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    @Unroll
    def "write read #nr"() {
        given:
        def perMaSet = WritabePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        when:
        perMaSet.set().addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritabePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        then:
        perMaSetReread.set().equals(set)

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
        def perMaSet = WritabePerMaSet.loadOrCreateStringSet(tempDir, 'testset')

        when:
        perMaSet.set().addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritabePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        then:
        perMaSetReread.set().equals(set)
    }

    def "write read readOnly string set"() {
        given:
        def set = ['foo', 'N I X', 'bla bla bla'] as Set
        def perMaSet = WritabePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        perMaSet.set().addAll(set)
        perMaSet.persist();
        def perMaReread = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")

        then:
        perMaReread.set().equals(set)
    }

    def "read readOnly string no files"() {
        when:
        def perMaSetReread = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")

        then:
        perMaSetReread.set().equals([] as Set)
    }

    @Unroll
    def "write read write update readOnly string set #initial #update"() {
        given:
        def writablePerMaSet = WritabePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        when:
        writablePerMaSet.set().addAll(initial)
        writablePerMaSet.persist()
        def readOnlyPerMaSet = ReadOnlyPerMaSet.loadStringSet(tempDir, "testset")
        writablePerMaSet.set().clear()
        writablePerMaSet.set().addAll(update)
        writablePerMaSet.persist()
        readOnlyPerMaSet.refresh()

        then:
        readOnlyPerMaSet.set().equals(update)

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
        def perMaSet = WritabePerMaSet.loadOrCreateStringSet(tempDir, "testmap")

        when:
        [['foo'],
         ['foo', 'N I X'],
         ['foo', 'N I X', 'long store'],
         ['N I X', 'long store']].forEach {
            perMaSet.persist()
            perMaSet.set().clear()
            perMaSet.set().addAll(it)
        }
        perMaSet.compact()
        def perMaRereadSet = ReadOnlyPerMaSet.loadStringSet(tempDir, "testmap")

        then:
        perMaSet.set().equals(['N I X','long store'] as Set)
        perMaRereadSet.set().equals(['N I X','long store'] as Set)
    }
}