/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate

import static ch.sbb.perma.datastore.KeyOrValueSerializer.*

class PerMaSetTest extends Specification {
    private static String LONG_STRING = 'the quick brown fox jumped over the lazy cat'.multiply(99999)
    File tempDir

    def setup() {
        tempDir = File.createTempDir()
    }

    @Unroll
    def "write read #nr"() {
        given:
        def perMaSet = WritabePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        when:
        perMaSet.set().addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritabePerMaSet.loadOrCreate(tempDir, "testset", serializer)

        then:
        perMaSetReread.set() == set

        where:
        nr | set                                   | serializer
        1  | ['foo'] as Set                        | STRING
        2  | [] as Set                             | STRING
        3  | ['foo', 'b A r', LONG_STRING] as Set  | STRING
        4  | [7,42,9999,66666] as Set              | INTEGER
        5  | [999999999999L] as Set                | LONG
        6  | [LocalDate.MAX, LocalDate.MIN] as Set | JAVA_OBJECT
    }

    def "write read string set"() {
        given:
        def set = ['foo', 'bar'] as Set
        def perMaSet = WritabePerMaSet.loadOrCreateStringSet(tempDir, 'testset')

        when:
        perMaSet.set().addAll(set)
        perMaSet.persist();
        def perMaSetReread = WritabePerMaSet.loadOrCreateStringSet(tempDir, "testset")

        then:
        perMaSetReread.set() == set
    }

}