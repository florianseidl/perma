/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import ch.sbb.perma.datastore.HeaderMismatchException
import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.datastore.KeyOrValueSerializer.*

class MapSnapshotTest extends Specification {
    private static String VALUE_A = 'value A'
    private static String VALUE_B = 'nix als bledsinn for value B'
    private static String VALUE_C = 'immer wieder value C -'.multiply(99999)

    private File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def "new snapshot"() {
        when:
        def snapshot = new NewMapSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                STRING,
                STRING)

        then:
        snapshot.asImmutableMap() == [:]
    }

    @Unroll
    def "next from new snapshot #map.keySet()"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                keySerializer,
                valueSerializer)

        when:
        def next = newSnapshot.writeNext(map)

        then:
        next.asImmutableMap() == map
        new Directory(tempDir, 'foo').fileExists()

        where:
        map                                     | keySerializer  | valueSerializer
        ['A':VALUE_A]                           | STRING         | STRING
        ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C] | STRING         | STRING
        [:]                                     | STRING         | STRING
        ['foo':'bar']                           | STRING         | STRING
        [666:42]                                | INTEGER        | INTEGER
        [99999L : Long.MIN_VALUE]               | LONG           | LONG
    }


    @Unroll
    def "next from persisted snapshot #map.keySet()"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                keySerializer,
                valueSerializer)

        when:
        def next = newSnapshot.writeNext([:])
        def overnext = next.writeNext(map)

        then:
        overnext.asImmutableMap() == map

        where:
        map                                     | keySerializer | valueSerializer
        ['A':VALUE_A]                           | STRING        | STRING
        ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C] | STRING        | STRING
        [:]                                     | STRING        | STRING
        [3:'bar']                               | INTEGER       | STRING
        ['bar':3]                               | STRING        | INTEGER
        [99999L : Long.MIN_VALUE]               | LONG          | LONG
    }


    @Unroll
    def "many persisted states #stateKeys"() {
        given:
        def next = new NewMapSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                STRING,
                STRING)

        when:
        for(def state : states) {
            next = next.writeNext(state)
        }

        then:
        next.asImmutableMap() == states[-1]

        where:
        states << [[['A':VALUE_A]],
                   [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C]],
                   [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_A, 'C':VALUE_C]],
                   [['A':VALUE_A, 'B':VALUE_B], [:]],
                   [[:],['A':VALUE_A, 'B':VALUE_B]]]
        stateKeys = states.collect{it.keySet()}
    }

    def "next from persisted snapshot delelete intermediate snapshot"() {
        given:
        def directory = new Directory(tempDir, 'foo')
        def newSnapshot = new NewMapSnapshot(
                'foo', directory, STRING, STRING)

        when:
        newSnapshot.writeNext([:])
        .writeNext(['A':VALUE_A])
        .writeNext(['A':VALUE_A, 'B':VALUE_B]);

        directory.listLatest().deltaFiles()[0].delete()

        PersistendMapSnapshot.loadLatest(directory, STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "snapshot file from different full file"() {
        given:
        def newSnapshotFoo = new NewMapSnapshot(
                'foo', new Directory(tempDir, 'foo'), STRING, STRING)
        def newSnapshotBar = new NewMapSnapshot(
                'bar', new Directory(tempDir, 'bar'), STRING, STRING)

        when:
        newSnapshotFoo.writeNext(['A':VALUE_A]).writeNext(['C':VALUE_C])
        newSnapshotBar.writeNext([:]).writeNext(['C':VALUE_C]).writeNext(['C':VALUE_C, 'B':VALUE_B]);

        File firstFooDelta = new Directory(tempDir, 'foo').listLatest().deltaFiles()[0]
        File lastBarDelta = new Directory(tempDir, 'bar').listLatest().deltaFiles()[1]
        lastBarDelta.delete()
        firstFooDelta.renameTo(lastBarDelta)

        PersistendMapSnapshot.loadLatest(new Directory(tempDir, 'bar'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "load not found"() {
        given:
        def directory = new Directory(tempDir, 'foo')

        when:
        PersistendMapSnapshot.loadLatest(directory, STRING, STRING)

        then:
        thrown FileNotFoundException
    }
}