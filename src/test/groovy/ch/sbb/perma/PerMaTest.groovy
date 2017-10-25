/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import ch.sbb.perma.datastore.ImmutableListSerializer
import ch.sbb.perma.datastore.ImmutableSetSerializer
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.datastore.KeyOrValueSerializer.*

class PerMaTest extends Specification {
    private static String FOO = 'foobar'
    private static String NIX = 'nix als bledsinn'
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)

    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    @Unroll
    def "write read #map.keySet()"() {
        given:
        def perMa = WritabePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        when:
        perMa.map().putAll(map)
        perMa.persist();
        def perMaReread = WritabePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        then:
        perMaReread.map().equals(map)

        where:
        map                                               | keySerializer | valueSerializer
        ['foo':FOO]                                       | STRING        | STRING
        ['fooo':FOO, 'N I X':NIX]                         | STRING        | STRING
        [:]                                               | STRING        | STRING
        ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING] | STRING        | STRING
        [7:42, 9999:66666]                                | INTEGER       | INTEGER
        [999999999999L:Long.MAX_VALUE]                    | LONG          | LONG
        ['foo' : new Date(0)]                             | STRING        | JAVA_OBJECT
        [(LocalDate.MAX) : LocalDate.MIN]                 | JAVA_OBJECT   | JAVA_OBJECT
    }

    @Unroll
    def "write read collection value #nr"() {
        given:
        def perMa = WritabePerMa.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        when:
        perMa.map().putAll(map)
        perMa.persist();
        def perMaReread = WritabePerMa.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        then:
        perMaReread.map()['key'].equals(expected)

        where:
        nr | map                                                        | valueSerializer                       || expected
        1  | ['key':ImmutableList.copyOf([FOO, FOO, NIX, LONG_STRING])] | new ImmutableListSerializer<>(STRING) || [FOO, FOO, NIX, LONG_STRING]
        2  | ['key':ImmutableSet.of()]                                  | new ImmutableListSerializer<>(STRING) || []
        3  | ['key':ImmutableSet.copyOf([FOO, FOO, NIX, LONG_STRING])]  | new ImmutableSetSerializer<>(STRING)  || [FOO, NIX, LONG_STRING] as Set
        4  | ['key':ImmutableSet.copyOf([1, 1, 2])]                     | new ImmutableSetSerializer<>(INTEGER) || [1, 2] as Set
        5  | ['key':ImmutableSet.of()]                                  | new ImmutableSetSerializer<>(STRING)  || [] as Set
    }

    def "write read string2String map"() {
        given:
        def map = ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
        def perMa = WritabePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perMa.map().putAll(map)
        perMa.persist();
        def perMaReread = WritabePerMa.loadOrCreateStringMap(tempDir, "testmap")

        then:
        perMaReread.map().equals(map)
    }

    def "write read readOnly string2String map"() {
        given:
        def map = ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
        def perMa = WritabePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perMa.map().putAll(map)
        perMa.persist();
        def perMaReread = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")

        then:
        perMaReread.map().equals(map)
    }

    def "read readOnly string2String no files"() {
        when:
        def perMaReread = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")

        then:
        perMaReread.map().equals([:])
    }

    @Unroll
    def "write read write update readOnly string2String map #initial.keySet() #update.keySet()"() {
        given:
        def writablePerMa = WritabePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        writablePerMa.map().putAll(initial)
        writablePerMa.persist()
        def readOnlyPerMa = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")
        writablePerMa.map().clear()
        writablePerMa.map().putAll(update)
        writablePerMa.persist()
        readOnlyPerMa.udpate()

        then:
        readOnlyPerMa.map().equals(update)

        where:
        initial                                            | update
        ['foo':FOO]                                        | ['foo':FOO, 'N I X':NIX]
        ['foo':FOO, 'N I X':NIX]                           | ['foo':LONG_STRING, 'N I X':FOO]
        ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING] | [:]
        ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING] | ['foo':NIX, 'long store':LONG_STRING]
        [:]                                                | [:]
        [:]                                                | ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
    }

}
