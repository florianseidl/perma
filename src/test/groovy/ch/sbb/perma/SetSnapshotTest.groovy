/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Specification
import spock.lang.Unroll

import static ch.sbb.perma.datastore.KeyOrValueSerializer.*

class SetSnapshotTest extends Specification {
    private static String VALUE_A = 'value A'
    private static String VALUE_B = 'nix als bledsinn for value B'
    private static String VALUE_C = 'immer wieder value C -'.multiply(99999)

    private File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    def "new snapshot"() {
        when:
        def snapshot = new NewSetSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                STRING)

        then:
        snapshot.asImmutableSet() == [] as Set
    }

    @Unroll
    def "next from new snapshot #set"() {
        given:
        def newSnapshot = new NewSetSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                serializer)

        when:
        def next = newSnapshot.writeNext(set)

        then:
        next.asImmutableSet() == set
        new Directory(tempDir, 'foo').fileExists()

        where:
        set                             | serializer
        ['A'] as Set                    | STRING
        ['A','B','C'] as Set            | STRING
        [] as Set                       | STRING
        [666,42] as Set                 | INTEGER
        [99999L, Long.MIN_VALUE] as Set | LONG
    }


    @Unroll
    def "next from persisted snapshot #set"() {
        given:
        def newSnapshot = new NewSetSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                serializer)

        when:
        def next = newSnapshot.writeNext([] as Set)
        def overnext = next.writeNext(set)

        then:
        overnext.asImmutableSet() == set

        where:
        set                             | serializer
        ['A'] as Set                    | STRING
        ['A','B','C'] as Set            | STRING
        [] as Set                       | STRING
        [3,4,5] as Set                  | INTEGER
        [99999L, Long.MIN_VALUE] as Set | LONG
    }


    @Unroll
    def "load from many persisted #states"() {
        given:
        def next = new NewSetSnapshot(
                'foo',
                new Directory(tempDir, 'foo'),
                STRING)

        when:
        for(def state : states) {
            next = next.writeNext(state)
        }

        then:
        next.asImmutableSet() == states[-1]

        where:
        states << [[['A'] as Set],
                   [['A'] as Set, ['A', 'B', 'C'] as Set],
                   [['A'] as Set, ['A', 'B', 'C'] as Set, ['A', 'C'] as Set],
                   [['A', 'B'] as Set, [] as Set],
                   [[] as Set,['A', 'B'] as Set]]
    }
}