/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import org.javatuples.Pair
import org.javatuples.Triplet
import org.javatuples.Unit
import spock.lang.Specification
import spock.lang.Unroll

import java.time.*

import static java.util.Optional.empty
import static java.util.Optional.of

class SerializerTest extends Specification {

    @Unroll
    def "serialize deseralize #serializer.class.simpleName #value"() {
        when:
        def valueDeseralized = serializer.fromByteArray(serializer.toByteArray(value))

        then:
        valueDeseralized.equals(value)

        where:
        serializer                                                  | value
        new StringSerializer()                                      | 'foo bar'
        new StringSerializer()                                      | ''
        new StringSerializer()                                      | ' '
        new IntegerSerializer()                                     | Integer.MAX_VALUE
        new IntegerSerializer()                                     | 0
        new IntegerSerializer()                                     | Integer.MIN_VALUE
        new LongSerializer()                                        | Long.MAX_VALUE
        new LongSerializer()                                        | 0L
        new LongSerializer()                                        | Long.MIN_VALUE
        new ByteSerializer()                                        | Byte.MAX_VALUE
        new ByteSerializer()                                        | Byte.MIN_VALUE
        new ShortSerializer()                                       | Short.MAX_VALUE
        new ShortSerializer()                                       | Short.MIN_VALUE
        new FloatSerializer()                                       | Float.MAX_VALUE
        new FloatSerializer()                                       | 0.0f
        new FloatSerializer()                                       | Float.MIN_VALUE
        new DoubleSerializer()                                      | Double.MAX_VALUE
        new DoubleSerializer()                                      | 0.0d
        new DoubleSerializer()                                      | Double.MIN_VALUE
        new BigDecimalSerializer()                                  | BigDecimal.ZERO
        new BigDecimalSerializer()                                  | new BigDecimal('9999999999999999999999999999999999999.00000000000000000000000000000000000000001')
        new BigIntegerSerializer()                                  | BigInteger.ZERO
        new BigIntegerSerializer()                                  | new BigInteger('99999999999999999999999999999999999999999999999999999999999987654321')
        new CharSerializer()                                        | 'a' as Character
        new CharSerializer()                                        | ' ' as Character
        new CharSerializer()                                        | 'Ü' as Character
        new ShortStringSerializer()                                 | 'bli bla blo'
        new ShortStringSerializer()                                 | ''
        new OptionalStringSerializer()                              | of("foo")
        new OptionalStringSerializer()                              | empty()
        new ImmutableListSerializer(new StringSerializer())         | ImmutableList.builder().addAll(['a', 'b', 'c']).build()
        new ImmutableListSerializer(new StringSerializer())         | ImmutableList.builder().build()
        new ImmutableSetSerializer(new IntegerSerializer())         | ImmutableSet.builder().addAll([1, 2, 3] as Set).build()
        new ImmutableSetSerializer(new IntegerSerializer())         | ImmutableSet.builder().build()
        new ImmutableListSerializer(new OptionalStringSerializer()) | ImmutableList.builder().addAll([of('bla'), of('blo')]).build()
        new ImmutableListSerializer(new OptionalStringSerializer()) | ImmutableList.builder().addAll([empty()] as Set).build()
        new PairSerializer(new StringSerializer(),
                new IntegerSerializer())                            | new Pair('foo', 42)
        new PairSerializer(new StringSerializer(),
                new IntegerSerializer())                            | new Pair('foo', null)
        new TripletSerializer(new StringSerializer(),
                new IntegerSerializer(),
                new LongSerializer())                               | new Triplet('foo', 42, 111L)
        new TripletSerializer(new StringSerializer(),
                new IntegerSerializer(),
                new StringSerializer())                             | new Triplet(null, 42, 'bar')
        new UnitSerializer(new IntegerSerializer())                 | new Unit(42)
        new LocalDateSerializer()                                   | LocalDate.MAX
        new LocalTimeSerializer()                                   | LocalTime.MAX
        new LocalDateSerializer()                                   | LocalDate.MIN
        new LocalTimeSerializer()                                   | LocalTime.MIN
        new LocalDateTimeSerializer()                               | LocalDateTime.MAX
        new LocalDateTimeSerializer()                               | LocalDateTime.MIN
        new ZonedDateTimeSerializer()                               | ZonedDateTime.of(LocalDateTime.MAX, ZoneId.of(ZoneId.SHORT_IDS.get('ECT')))
        new ZonedDateTimeSerializer()                               | ZonedDateTime.of(LocalDateTime.MIN, ZoneOffset.UTC)
        new OffsetDateTimeSerializer()                              | OffsetDateTime.MAX
        new OffsetDateTimeSerializer()                              | OffsetDateTime.MIN
        new DateSerializer()                                        | new Date(Long.MAX_VALUE)
        new DateSerializer()                                        | new Date(0)

    }

    def "serialize to null not allowed for tuples"() {
        when:
        new PairSerializer(
                new StringSerializer(),
                new OptionalStringSerializer())
                .toByteArray(new Pair("bar", empty()))

        then:
        thrown IllegalArgumentException
    }
}