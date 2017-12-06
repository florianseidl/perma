/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import ch.sbb.perma.serializers.ImmutableListSerializer
import ch.sbb.perma.serializers.ImmutableSetSerializer
import ch.sbb.perma.serializers.PairSerializer
import ch.sbb.perma.serializers.TripletSerializer
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import org.javatuples.Pair
import org.javatuples.Triplet
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.serializers.KeyOrValueSerializer.*

class PerMaTest extends Specification {
    private static String FOO = 'foobar'
    private static String NIX = 'nix als bledsinn'
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)

    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def cleanup() {
        tempDir.deleteDir()
    }

    @Unroll
    def "write read #map.keySet()"() {
        given:
        def perMa = WritablePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        when:
        perMa.putAll(map)
        perMa.persist()
        def perMaReread = WritablePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        then:
        perMaReread.equals(map)

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
        def perMa = WritablePerMa.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        when:
        perMa.putAll(map)
        perMa.persist()
        def perMaReread = WritablePerMa.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        then:
        perMaReread['key'].equals(expected)

        where:
        nr | map                                                        | valueSerializer                       || expected
        1  | ['key':ImmutableList.copyOf([FOO, FOO, NIX, LONG_STRING])] | new ImmutableListSerializer<>(STRING) || [FOO, FOO, NIX, LONG_STRING]
        2  | ['key':ImmutableSet.of()]                                  | new ImmutableListSerializer<>(STRING) || []
        3  | ['key':ImmutableSet.copyOf([FOO, FOO, NIX, LONG_STRING])]  | new ImmutableSetSerializer<>(STRING)  || [FOO, NIX, LONG_STRING] as Set
        4  | ['key':ImmutableSet.copyOf([1, 1, 2])]                     | new ImmutableSetSerializer<>(INTEGER) || [1, 2] as Set
        5  | ['key':ImmutableSet.of()]                                  | new ImmutableSetSerializer<>(STRING)  || [] as Set
    }

    @Unroll
    def "write read tuple value #map.keySet()"() {
        given:
        def perMa = WritablePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        when:
        perMa.putAll(map)
        perMa.persist()
        def perMaReread = WritablePerMa.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        then:
        perMaReread.equals(map)

        where:
        map                          | keySerializer                                                         | valueSerializer
        ['key':new Pair(1, 'A')]     | STRING                                                                | new PairSerializer<Integer, String>(INTEGER, STRING)
        [(new Pair('X',42L)):7]      | new PairSerializer<Integer, String>(STRING, LONG)                     | INTEGER
        [(new Pair('X',null)):7]     | new PairSerializer<Integer, String>(STRING, LONG)                     | INTEGER
        [(new Pair(null,null)):7]    | new PairSerializer<Integer, String>(STRING, LONG)                     | INTEGER
        [(new Triplet('A',1,2)):32]  | new TripletSerializer<String,Integer,Integer>(STRING,INTEGER,INTEGER) | INTEGER
    }

    def "write read string map"() {
        given:
        def map = ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
        def perMa = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perMa.putAll(map)
        perMa.persist();
        def perMaReread = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        then:
        perMaReread.equals(map)
    }

    def "write read readOnly string map"() {
        given:
        def map = ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
        def perMa = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perMa.putAll(map)
        perMa.persist()
        def perMaReread = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")

        then:
        perMaReread.equals(map)
    }

    def "read readOnly string2String no files"() {
        when:
        def perMaReread = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")

        then:
        perMaReread.equals([:])
    }

    @Unroll
    def "write read write refresh readOnly string map #initial.keySet() #update.keySet()"() {
        given:
        def writablePerMa = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        writablePerMa.putAll(initial)
        writablePerMa.persist()
        def readOnlyPerMa = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")
        writablePerMa.clear()
        writablePerMa.putAll(update)
        writablePerMa.persist()
        readOnlyPerMa.refresh()

        then:
        readOnlyPerMa.equals(update)

        where:
        initial                                            | update
        ['foo':FOO]                                        | ['foo':FOO, 'N I X':NIX]
        ['foo':FOO, 'N I X':NIX]                           | ['foo':LONG_STRING, 'N I X':FOO]
        ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING] | [:]
        ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING] | ['foo':NIX, 'long store':LONG_STRING]
        [:]                                                | [:]
        [:]                                                | ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
    }

    def "write read write refresh with compact readOnly string map"() {
        given:
        def writablePerMa = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        writablePerMa.putAll(['foo':FOO])
        writablePerMa.persist()
        def readOnlyPerMa = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")
        [['foo':FOO, 'N I X':NIX],
         ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING],
         ['N I X':NIX, 'long store':LONG_STRING]].forEach {
            writablePerMa.clear()
            writablePerMa.putAll(it)
            writablePerMa.persist()
        }
        writablePerMa.compact()
        readOnlyPerMa.refresh()

        then:
        readOnlyPerMa.equals(['N I X':NIX, 'long store':LONG_STRING])
    }

    def "write compact reread string map"() {
        given:
        def perMa = WritablePerMa.loadOrCreateStringMap(tempDir, "testmap")

        when:
        [['foo':FOO],
         ['foo':FOO, 'N I X':NIX],
         ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING],
         ['N I X':NIX, 'long store':LONG_STRING]].forEach {
            perMa.persist()
            perMa.clear()
            perMa.putAll(it)
        }
        perMa.compact()
        def perMaReread = ReadOnlyPerMa.loadStringMap(tempDir, "testmap")

        then:
        perMa.equals(['N I X':NIX, 'long store':LONG_STRING])
        perMaReread.equals(['N I X':NIX, 'long store':LONG_STRING])
    }
}