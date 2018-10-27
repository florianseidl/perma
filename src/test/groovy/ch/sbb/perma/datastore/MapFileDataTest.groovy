/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore

import ch.sbb.perma.file.GZipCompression
import ch.sbb.perma.file.NoCompression
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.serializers.KeyOrValueSerializer.STRING

class MapFileDataTest extends Specification {
    private static final String NAME = "testmap";
    private static String VALUE_A = 'value A'
    private static String VALUE_B = 'nix als bledsinn for value B'
    private static String VALUE_C = 'immer wieder value C -'.multiply(99999)
    

    def length(Header header) {
        def out = new ByteArrayOutputStream()
        header.writeTo(out)
        return out.toByteArray().length
    }

    def write() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, 1),
                ImmutableMap.copyOf(['A': VALUE_A]),
                ImmutableSet.of()
        )
                .writeTo(out, STRING, STRING)

        then:
        out.toByteArray().length > 0
    }

    @Unroll
    def "write read #map.keySet() compression #compression"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, map.size()),
                ImmutableMap.copyOf(map),
                ImmutableSet.of()
        )
                .writeTo(out, STRING, STRING)
        def reread = MapFileData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread).equals(map)

        where:
        map << [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B], [:], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C]] * 2
        compression << [new NoCompression()] * 4 + [new GZipCompression()] * 4
    }

    @Unroll
    def "write manipulate read #b"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, 2),
                ImmutableMap.copyOf(['A': VALUE_A, 'B': VALUE_B]),
                ImmutableSet.of()
        )
                .writeTo(out, STRING, STRING)
        MapFileData.readFrom(
                new ByteArrayInputStream(manipulate(out.toByteArray(),
                                         length(Header.newFullHeader(NAME, 2)) + b)),
                STRING,
                STRING)

        then:
        thrown InvalidDataException

        where:
        b << [19,18,21,1]
    }

    byte[] manipulate(byte[] bytes, at) {
        byte[] copy = Arrays.copyOf(bytes, bytes.length)
        copy[at] = 0xF9
        return copy;
    }

    @Unroll
    def "write deleted #deleted"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(
                Header.newFullHeader(NAME, deleted.size()),
                ImmutableMap.of(),
                ImmutableSet.copyOf(deleted as Set)
        )
                .writeTo(out, STRING, STRING)
        def reread = MapFileData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread, existingMap).equals(expected)

        where:
        deleted    | existingMap                  || expected
        ['A']      | ['A': VALUE_A, 'B': VALUE_B] || ['B': VALUE_B]
        ['A','B']  | ['A': VALUE_A, 'B': VALUE_B] || [:]
        []         | ['A': VALUE_A, 'B': VALUE_B] || ['A': VALUE_A, 'B': VALUE_B]
        ['B', 'C'] | ['A': VALUE_A, 'B': VALUE_B] || ['A': VALUE_A]
        ['A']      | [:]                          || [:]
        []         | [:]                          || [:]
    }

    def "mixed new and deleted #newOrUpdated #deleted"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, newOrUpdated.size() + deleted.size()),
                ImmutableMap.copyOf(newOrUpdated),
                ImmutableSet.copyOf(deleted as Set)
        )
                .writeTo(out, STRING, STRING)
        def reread = MapFileData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread).equals(expected)

        where:
        newOrUpdated                                       | deleted || expected
        ['A':VALUE_A, 'B':VALUE_B] | ['C']   || ['A':VALUE_A, 'B':VALUE_B]
    }

    def sizeMismatchOnWrite() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, 1),
                ImmutableMap.copyOf(['A': VALUE_A, 'B': VALUE_B]),
                ImmutableSet.of()
        )
                .writeTo(out, STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def sizeMismatchOnRead() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapFileData(Header.newFullHeader(NAME, 2),
                ImmutableMap.copyOf(['A': VALUE_A, 'B': VALUE_B]),
                ImmutableSet.of()
        )
                .writeTo(out, STRING, STRING)
        def otherOut = new ByteArrayOutputStream()
        Header.newFullHeader(NAME, 1).writeTo(otherOut)
        def merged = out.toByteArray()
        mergeByteArrays(otherOut.toByteArray(), merged)
        MapFileData.readFrom(
                new ByteArrayInputStream(merged),
                STRING,
                STRING)

        then:
        thrown HeaderMismatchException
    }

    def mergeByteArrays(byte[] source, byte[] target) {
        System.arraycopy(source, 0, target, 0, source.size())
    }


    def extractMap(mapData, map=[:]) {
        mapData.addTo(map)
        return map
    }

    def toStringIsImplemented() {
        when:
        def mapDataToString = new MapFileData(
                Header.newFullHeader(NAME, 2),
                ImmutableMap.of('A', VALUE_A),
                ImmutableSet.of('C')
        )
                .toString();

        then:
        !mapDataToString.contains('@')
    }
}