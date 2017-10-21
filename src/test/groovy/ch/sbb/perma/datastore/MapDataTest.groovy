/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore

import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.datastore.KeyOrValueSerializer.STRING

class MapDataTest extends Specification {
    private static final UUID uuid = UUID.fromString("d4b89e8b-49d8-4c36-a798-942fb025a402");
    private static final String NAME = "testmap";
    private static String VALUE_A = 'value A'
    private static String VALUE_B = 'nix als bledsinn for value B'
    private static String VALUE_C = 'immer wieder value C -'.multiply(99999)
    

    def length(Header header) {
        def out = new ByteArrayOutputStream();
        header.writeTo(out);
        return out.toByteArray().length;
    }

    def write() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapData(Header.newFullHeader(NAME),
                    ImmutableMap.copyOf(['A': MapDataTest.VALUE_A]),
                    ImmutableSet.of())
                .writeTo(out, STRING, STRING)

        then:
        out.toByteArray().length > 0
    }


    @Unroll
    def "write read #map.keySet()"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapData(Header.newFullHeader(NAME),
                    ImmutableMap.copyOf(map),
                    ImmutableSet.of())
                .writeTo(out, STRING, STRING)
        def reread = MapData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread) == map

        where:
        map << [['A':MapDataTest.VALUE_A], ['A':MapDataTest.VALUE_A, 'B':MapDataTest.VALUE_B], [:], ['A':MapDataTest.VALUE_A, 'B':MapDataTest.VALUE_B, 'C':MapDataTest.VALUE_C]]
    }

    @Unroll
    def "write manipulate read #b"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapData(Header.newFullHeader(NAME),
                    ImmutableMap.copyOf(['A':MapDataTest.VALUE_A, 'B':MapDataTest.VALUE_B]),
                    ImmutableSet.of())
                .writeTo(out, STRING, STRING)
        MapData.readFrom(
                new ByteArrayInputStream(manipulate(out.toByteArray(),
                                         length(Header.newFullHeader(NAME)) + b)),
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
        new MapData(Header.newFullHeader(NAME),
                    ImmutableMap.of(),
                    ImmutableSet.copyOf(deleted as Set))
                .writeTo(out, STRING, STRING)
        def reread = MapData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread, existingMap) == expected

        where:
        deleted    | existingMap                                          || expected
        ['A']      | ['A': MapDataTest.VALUE_A, 'B': MapDataTest.VALUE_B] || ['B': MapDataTest.VALUE_B]
        ['A','B']  | ['A': MapDataTest.VALUE_A, 'B': MapDataTest.VALUE_B] || [:]
        []         | ['A': MapDataTest.VALUE_A, 'B': MapDataTest.VALUE_B] || ['A': MapDataTest.VALUE_A, 'B': MapDataTest.VALUE_B]
        ['B', 'C'] | ['A': MapDataTest.VALUE_A, 'B': MapDataTest.VALUE_B] || ['A': MapDataTest.VALUE_A]
        ['A']      | [:]                                                  || [:]
        []         | [:]                                                  || [:]
    }

    def "mixed new and deleted #newOrUpdated #deleted"() {
        given:
        def out = new ByteArrayOutputStream();

        when:
        new MapData(Header.newFullHeader(NAME),
                    ImmutableMap.copyOf(newOrUpdated),
                    ImmutableSet.copyOf(deleted as Set))
                .writeTo(out, STRING, STRING)
        def reread = MapData.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                STRING,
                STRING)

        then:
        extractMap(reread) == expected

        where:
        newOrUpdated                                       | deleted || expected
        ['A':MapDataTest.VALUE_A, 'B':MapDataTest.VALUE_B] | ['C']   || ['A':MapDataTest.VALUE_A, 'B':MapDataTest.VALUE_B]
    }

    def extractMap(mapData, map=[:]) {
        mapData.addTo(map);
        return map
    }
}
