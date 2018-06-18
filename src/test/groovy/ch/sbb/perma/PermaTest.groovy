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

class PermaTest extends Specification {
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
    def "write read #map.keySet() options #options"() {
        given:
        def perma = WritablePerma.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer, options)

        when:
        perma.putAll(map)
        perma.persist()
        def permaReread = WritablePerma.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer, options)

        then:
        permaReread.equals(map)

        where:
        map                                              | keySerializer | valueSerializer | options
        ['foo':FOO]                                      | STRING        | STRING          | Options.defaults()
        ['fooo':FOO,'N I X':NIX]                         | STRING        | STRING          | Options.defaults()
        [:]                                              | STRING        | STRING          | Options.defaults()
        ['foo':FOO,'N I X':NIX,'long store':LONG_STRING] | STRING        | STRING          | Options.defaults()
        [7:42, 9999:66666]                               | INTEGER       | INTEGER         | Options.defaults()
        [999999999999L:Long.MAX_VALUE]                   | LONG          | LONG            | Options.defaults()
        ['foo' : new Date(0)]                            | STRING        | JAVA_OBJECT     | Options.defaults()
        [(LocalDate.MAX) : LocalDate.MIN]                | JAVA_OBJECT   | JAVA_OBJECT     | Options.defaults()
        ['foo':FOO]                                      | STRING        | STRING          | new Options.Builder().compress(true).build()
    }

    @Unroll
    def "write read collection value #nr"() {
        given:
        def perma = WritablePerma.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        when:
        perma.putAll(map)
        perma.persist()
        def permaReread = WritablePerma.loadOrCreate(tempDir, "testmap", STRING, valueSerializer)

        then:
        permaReread['key'].equals(expected)

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
        def perma = WritablePerma.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        when:
        perma.putAll(map)
        perma.persist()
        def permaReread = WritablePerma.loadOrCreate(tempDir, "testmap", keySerializer, valueSerializer)

        then:
        permaReread.equals(map)

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
        def perma = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perma.putAll(map)
        perma.persist();
        def permaReread = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        then:
        permaReread.equals(map)
    }

    def "write read readOnly string map"() {
        given:
        def map = ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING]
        def perma = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        when:
        perma.putAll(map)
        perma.persist()
        def permaReread = ReadOnlyPerma.loadStringMap(tempDir, "testmap")

        then:
        permaReread.equals(map)
    }

    def "read readOnly string2String no files"() {
        when:
        def permaReread = ReadOnlyPerma.loadStringMap(tempDir, "testmap")

        then:
        permaReread.equals([:])
    }

    @Unroll
    def "write read write refresh readOnly string map #initial.keySet() #update.keySet()"() {
        given:
        def writablePerma = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        when:
        writablePerma.putAll(initial)
        writablePerma.persist()
        def readOnlyPerma = ReadOnlyPerma.loadStringMap(tempDir, "testmap")
        writablePerma.clear()
        writablePerma.putAll(update)
        writablePerma.persist()
        readOnlyPerma.refresh()

        then:
        readOnlyPerma.equals(update)

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
        def writablePerma = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        when:
        writablePerma.putAll(['foo':FOO])
        writablePerma.persist()
        def readOnlyPerma = ReadOnlyPerma.loadStringMap(tempDir, "testmap")
        [['foo':FOO, 'N I X':NIX],
         ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING],
         ['N I X':NIX, 'long store':LONG_STRING]].forEach {
            writablePerma.clear()
            writablePerma.putAll(it)
            writablePerma.persist()
        }
        writablePerma.compact()
        readOnlyPerma.refresh()

        then:
        readOnlyPerma.equals(['N I X':NIX, 'long store':LONG_STRING])
    }

    def "write compact reread string map"() {
        given:
        def perma = WritablePerma.loadOrCreateStringMap(tempDir, "testmap")

        when:
        [['foo':FOO],
         ['foo':FOO, 'N I X':NIX],
         ['foo':FOO, 'N I X':NIX, 'long store':LONG_STRING],
         ['N I X':NIX, 'long store':LONG_STRING]].forEach {
            perma.persist()
            perma.clear()
            perma.putAll(it)
        }
        perma.compact()
        def permaReread = ReadOnlyPerma.loadStringMap(tempDir, "testmap")

        then:
        perma.equals(['N I X':NIX, 'long store':LONG_STRING])
        permaReread.equals(['N I X':NIX, 'long store':LONG_STRING])
    }
}