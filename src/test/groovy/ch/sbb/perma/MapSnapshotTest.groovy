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
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)

        then:
        snapshot.asImmutableMap().equals([:])
    }

    @Unroll
    def "next from new snapshot #map.keySet()"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                keySerializer,
                valueSerializer)

        when:
        def next = newSnapshot.writeNext(map)

        then:
        next.asImmutableMap().equals(map)
        FileGroup.list(tempDir, 'foo').exists()

        where:
        map                                     | keySerializer  | valueSerializer
        ['A':VALUE_A]                           | STRING         | STRING
        ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C] | STRING         | STRING
        ['foo':'bar']                           | STRING         | STRING
        [666:42]                                | INTEGER        | INTEGER
        [99999L : Long.MIN_VALUE]               | LONG           | LONG
    }


    @Unroll
    def "next from persisted snapshot #map.keySet()"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                keySerializer,
                valueSerializer)

        when:
        def next = newSnapshot.writeNext([:])
        def overnext = next.writeNext(map)

        then:
        overnext.asImmutableMap().equals(map)

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
                FileGroup.list(tempDir, 'foo'),
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
        def newSnapshot = new NewMapSnapshot(
                'foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)

        when:
        newSnapshot.writeNext(['A':VALUE_A])
                    .writeNext(['A':VALUE_A, 'B':VALUE_B])
                    .writeNext(['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C])

        def firstDelta = FileGroup.list(tempDir, 'foo').deltaFiles()[0]
        def secondDelta = FileGroup.list(tempDir, 'foo').deltaFiles()[1]
        firstDelta.delete()
        secondDelta.renameTo(firstDelta)

        PersistendMapSnapshot.load(FileGroup.list(tempDir, 'foo'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "snapshot file from different full file"() {
        given:
        def newSnapshotFoo = new NewMapSnapshot(
                'foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)
        def newSnapshotBar = new NewMapSnapshot(
                'bar', FileGroup.list(tempDir, 'bar'), STRING, STRING)

        when:
        newSnapshotFoo.writeNext(['A':VALUE_A]).writeNext(['C':VALUE_C])
        newSnapshotBar.writeNext(['A':VALUE_A]).writeNext(['C':VALUE_C]).writeNext(['C':VALUE_C, 'B':VALUE_B]);

        File firstFooDelta = FileGroup.list(tempDir, 'foo').deltaFiles()[0]
        File lastBarDelta = FileGroup.list(tempDir, 'bar').deltaFiles()[1]
        lastBarDelta.delete()
        firstFooDelta.renameTo(lastBarDelta)

        PersistendMapSnapshot.load(FileGroup.list(tempDir, 'bar'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "not a full file"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)

        when:
        newSnapshot.writeNext(['A':VALUE_A]).writeNext(['B':VALUE_B])

        def files = FileGroup.list(tempDir, 'foo')
        def firstDelta = files.deltaFiles()[0]
        files.fullFile().delete()
        firstDelta.renameTo(files.fullFile())

        PersistendMapSnapshot.load(FileGroup.list(tempDir, 'foo'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "load persistent not found"() {
        given:
        def files = FileGroup.list(tempDir, 'foo')

        when:
        PersistendMapSnapshot.load(files, STRING, STRING)

        then:
        thrown FileNotFoundException
    }

    @Unroll
    def "next no change not written #map.keySet()"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)

        when:
        def overovernext = newSnapshot.writeNext(map).writeNext(map).writeNext(map)
        def files = FileGroup.list(tempDir, 'foo')
        def nrDeltaFiles = files.deltaFiles().size()
        def fullFileExists = files.exists()

        then:
        overovernext.asImmutableMap().equals(map)
        nrDeltaFiles == 0
        fullFileExists == !map.isEmpty()

        where:
        map << [['A':VALUE_A],
                ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C],
                [:]]
    }

}