/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import ch.sbb.perma.datastore.HeaderMismatchException
import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.serializers.KeyOrValueSerializer.*

class MapSnapshotTest extends Specification {
    private static String VALUE_A = 'value A'
    private static String VALUE_B = 'nix als bledsinn for value B'
    private static String VALUE_C = 'immer wieder value C -'.multiply(99999)

    private File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def cleanup() {
        tempDir.deleteDir()
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
                   [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_A, 'C':VALUE_C], [:]],
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

        PersistedMapSnapshot.load('foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)

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
        newSnapshotFoo.writeNext(['A':VALUE_A]).writeNext(['A':VALUE_A,'C':VALUE_C])
        newSnapshotBar.writeNext(['A':VALUE_A]).writeNext(['A':VALUE_A,'C':VALUE_C]).writeNext(['A':VALUE_A,'C':VALUE_C, 'B':VALUE_B]);

        File firstFooDelta = FileGroup.list(tempDir, 'foo').deltaFiles()[0]
        File lastBarDelta = FileGroup.list(tempDir, 'bar').deltaFiles()[1]
        lastBarDelta.delete()
        firstFooDelta.renameTo(lastBarDelta)

        PersistedMapSnapshot.load('bar', FileGroup.list(tempDir, 'bar'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "not a full file"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)

        when:
        newSnapshot.writeNext(['A':VALUE_A]).writeNext(['A':VALUE_A, 'B':VALUE_B])

        def files = FileGroup.list(tempDir, 'foo')
        def firstDelta = files.deltaFiles()[0]
        files.fullFile().delete()
        firstDelta.renameTo(files.fullFile())

        PersistedMapSnapshot.load('foo', FileGroup.list(tempDir, 'foo'), STRING, STRING)

        then:
        thrown HeaderMismatchException
    }

    def "load persistent not found"() {
        given:
        def files = FileGroup.list(tempDir, 'foo')

        when:
        PersistedMapSnapshot.load('foo', files, STRING, STRING)

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

    @Unroll
    def "many persisted states and compact #stateKeys"() {
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
        def compacted = next.compact()
        def filesAfterCompact = FileGroup.list(tempDir, 'foo')
        def deltaFilesAfterCompact = filesAfterCompact.deltaFiles().size()
        def existsAfterCompact = filesAfterCompact.exists()

        then:
        compacted.asImmutableMap().equals(states[states.size() - 1])
        existsAfterCompact
        deltaFilesAfterCompact == 0

        where:
        states << [[['A':VALUE_A]],
                   [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C]],
                   [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_A, 'C':VALUE_C]],
                   [[:],['A':VALUE_A, 'B':VALUE_B]]]
        stateKeys = states.collect{ it.keySet() }
    }

    def "no file ever persisted no compact no file empty"() {
        given:
        def next = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)

        when:
        next = next.writeNext([:])
        def compacted = next.compact()
        def existsAfterCompact = FileGroup.list(tempDir, 'foo').exists()

        then:
        compacted.asImmutableMap().isEmpty()
        !existsAfterCompact
    }

    def "no compact empty after delete all"() {
        given:
        def next = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)

        when:
        for(def state : [['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C],[:]]) {
            next = next.writeNext(state)
        }
        def fullFileBeforeCompact = FileGroup.list(tempDir, 'foo').fullFile()
        def compacted = next.compact()
        def fullFileAfterCompact = FileGroup.list(tempDir, 'foo').fullFile()

        then:
        compacted.asImmutableMap().isEmpty()
        fullFileAfterCompact != fullFileBeforeCompact
        compacted.asImmutableMap().isEmpty()
    }

    @Unroll
    def "autocompact #stateKeys"() {
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
        def filesAfterCompact = FileGroup.list(tempDir, 'foo')
        def deltaFilesAfterCompact = filesAfterCompact.deltaFiles().size()
        def existsAfterCompact = filesAfterCompact.exists()

        then:
        next.asImmutableMap().equals(states[states.size() - 1])
        existsAfterCompact
        (deltaFilesAfterCompact == 0) == compacted

        where:
        states                                                                  || compacted
        [['A':VALUE_A],[:]]                                                     || true
        [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C],[:]]            || true
        [['A':VALUE_A], ['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_A]] || true
        [['A':VALUE_A, 'B':VALUE_B],['A':VALUE_A]]                              || true
        [['A':VALUE_A],['A':VALUE_A, 'B':VALUE_B]]                              || false
        [['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_A,'B':VALUE_B]]    || false
        [['A':VALUE_A, 'B':VALUE_B, 'C':VALUE_C], ['A':VALUE_B,'B':VALUE_C]]    || true
        [['A':VALUE_A, 'B':VALUE_B],['A':VALUE_A, 'C':VALUE_C]]                 || true
        stateKeys = states.collect{ it.keySet() }
    }

    @Unroll
    def "key or value serializer null"() {
        when:
        MapSnapshot.loadOrCreate(
                tempDir,
                'foo',
                keySerializer,
                valueSerialzer)

        then:
        thrown NullPointerException

        where:
        keySerializer  | valueSerialzer
        STRING         | null
        null           | STRING
    }

    def "delete tempfile on writeNext NewMapSnapshot"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)
        def oldTemp = FileGroup.list(tempDir, 'foo').createTempFile()
        oldTemp.write('irgendwas')

        when:
        newSnapshot.writeNext(['A':VALUE_A] )

        then:
        FileGroup.list(tempDir, 'foo').exists()
        !oldTemp.exists()
    }

    def "delete tempfile on writeNext PersistedMapSnapshot"() {
        given:
        def newSnapshot = new NewMapSnapshot(
                'foo',
                FileGroup.list(tempDir, 'foo'),
                STRING,
                STRING)
        def persistedSnapshot = newSnapshot.writeNext(['A':VALUE_A] )
        def oldTemp = FileGroup.list(tempDir, 'foo').createTempFile()
        oldTemp.write('irgendwas')

        when:
        persistedSnapshot.writeNext(['A':VALUE_A, 'B': VALUE_B] )

        then:
        FileGroup.list(tempDir, 'foo').exists()
        !oldTemp.exists()
    }

}