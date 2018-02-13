/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore

import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.serializers.KeyOrValueSerializer.*
import static ch.sbb.perma.serializers.NullValueSerializer.NULL

class MapEntryRecordTest extends Specification {

    @Unroll
    def "write read #entryKey"() {
        given:
        def out = new ByteArrayOutputStream()

        when:
        def record = MapEntryRecord.newOrUpdated(entryKey, entryValue)
        record.writeTo(
                out,
                keySerializer,
                valueSerializer)
        def reread = record.readFrom(
                new ByteArrayInputStream(out.toByteArray()),
                keySerializer,
                valueSerializer)

        then:
        extractNewOrUdpated(reread).equals(ImmutableMap.of(entryKey, entryValue))

        where:
        entryKey | entryValue     | keySerializer  | valueSerializer
        'foo'    | 'bar'          | STRING         | STRING
        ''       | 'bar'          | STRING         | STRING
        'foo'    | ''             | STRING         | STRING
        ''       | ''             | STRING         | STRING
        'foo'    | 42             | STRING         | INTEGER
        3        | Long.MAX_VALUE | INTEGER        | LONG
    }

    @Unroll
    def "write read invalid #key"() {
        given:
        def out = new ByteOutputStream()

        when:
        def record = MapEntryRecord.newOrUpdated(key, 'hotzenplotz')
        record.writeTo(
                out,
                STRING,
                STRING)
        def bytes = out.bytes
        bytes[at] = 0xF0
        record.readFrom(
                new ByteArrayInputStream(out.bytes),
                STRING,
                STRING)

        then:
        thrown InvalidDataException

        where:
        key   | at
        "aaa" | 15
        "bbb" | 12
        "ccc" | 0
        "ddd" | 0
        "eee" | 18
        "fff" | 23
        "ggg" | 4
    }

    def "write read deleted"() {
        given:
        def out = new ByteOutputStream()

        when:
        def record = MapEntryRecord.deleted('foo')
        record.writeTo(
                out,
                STRING,
                NULL)
        def reread = record.readFrom(
                        new ByteArrayInputStream(out.bytes),
                        STRING,
                        NULL)

        then:
        extractDeleted(reread).equals(['foo'] as Set)
    }

    @Unroll
    def "write read null value #value"() {
        given:
        def mapBuilder = new ImmutableMap.Builder<String, Optional<byte[]>>()
        def out = new ByteOutputStream()

        when:
        def record = MapEntryRecord.newOrUpdated('foo', value)
        record.writeTo(out, STRING, OPTIONAL_STRING)
        def reread = MapEntryRecord.readFrom(
                new ByteArrayInputStream(out.bytes),
                STRING,
                OPTIONAL_STRING)
        reread.addTo(mapBuilder, ImmutableSet.builder())

        then:
        def map = mapBuilder.build()
        map['foo'] == value

        where:
        value << [Optional.empty(), Optional.of("is eh was da")];
    }

    def extractNewOrUdpated(mapEntryRecord) {
        return extract(mapEntryRecord)[0]
    }

    def extractDeleted(mapEntryRecord) {
        return extract(mapEntryRecord)[1]
    }

    def extract(mapEntryRecord) {
        final newOrUpdated= new ImmutableMap.Builder<String, byte[]>()
        final deleted = new ImmutableSet.Builder<String>()
        mapEntryRecord.addTo(newOrUpdated, deleted);
        return [newOrUpdated.build(), deleted.build()]
    }

    def toStringIsImplemented() {
        when:
        def recordToString = MapEntryRecord.newOrUpdated('foo', 'bar').toString()

        then:
        !recordToString.contains('@')
    }

}
