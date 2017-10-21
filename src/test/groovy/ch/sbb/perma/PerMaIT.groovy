/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma

import com.google.common.collect.MapDifference
import com.google.common.collect.Maps
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

import static ch.sbb.perma.datastore.KeyOrValueSerializer.STRING
import static java.util.UUID.randomUUID

/**
 * Test the performance and memory usage with real load.
 * <p>
 * Warning: will require 10GB ram and disk space. Disabled by default in POM.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
class PerMaIT extends Specification {

    File tempDir

    def setup() {
        tempDir = createTempDir()
        println("Using temp dir: $tempDir")
    }

    def createTempDir() {
        String configuredTempDir = System.getProperty('tempDir')
        if(configuredTempDir == null) {
            return File.createTempDir()

        }
        File dir = new File(configuredTempDir, randomUUID().toString())
        dir.mkdirs()
        return dir;
    }

    def cleanup() {
        tempDir.deleteDir()
    }

    @Unroll
    @Timeout(value=5, unit=TimeUnit.MINUTES)
    def "write read #writes times map of #mapSize random entries"() {
        given:
        def perMa = WritabePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)

        when:
        (0 .. writes).forEach {
            (0..mapSize).forEach {
                perMa.map().put(randomUUID().toString(),
                                randomUUID().toString().multiply(50*(it%5 + 1)))
            }
            println("write $it, mapSize ${perMa.map().size()}")
            perMa.persist();
        }
        println("all written, mapSize ${perMa.map().size()}")
        def perMaReread = WritabePerMa.loadOrCreate(tempDir, "bigmap", STRING, STRING)
        println("reread, mapSize ${perMaReread.map().size()}")

        then:
        perMaReread.map().size() == perMa.map().size()
        MapDifference diff = Maps.difference(perMa.map(), perMaReread.map())
        assert diff.entriesDiffering().isEmpty()
        assert diff.entriesOnlyOnLeft().isEmpty()
        assert diff.entriesOnlyOnRight().isEmpty()

        where:
        writes | mapSize
        500    | 500
        50     | 5000
        5      | 50000

        resultSize = writes * mapSize
    }
}