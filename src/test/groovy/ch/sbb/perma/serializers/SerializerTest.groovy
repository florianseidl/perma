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

import java.nio.charset.Charset
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
        new StringSerializer(StringSerializer.UTF_16BE)             | 'foo bar'
        new StringSerializer(Charset.forName('ISO-8859-1'))         | 'foo bar'
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
        new CharacterSerializer()                                   | 'a' as Character
        new CharacterSerializer()                                   | ' ' as Character
        new CharacterSerializer()                                   | 'Ü' as Character
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
        new PairSerializer(new StringSerializer(),
                new StringSerializer())                             | new Pair('', '')
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
        new EnumSerializer(DayOfWeek.class)                         | DayOfWeek.FRIDAY
    }

    @Unroll
    def "serialize deseralize array #serializer.class.simpleName #value"() {
        when:
        def valueDeseralized = serializer.fromByteArray(serializer.toByteArray(value))

        then:
        Arrays.equals(valueDeseralized, value)

        where:
        serializer                                         | value
        new ObjectArraySerializer(
                Integer.class,
                KeyOrValueSerializer.INTEGER)              | [42, 7] as Integer[]
        new ObjectArraySerializer(
                Integer.class,
                KeyOrValueSerializer.INTEGER)              | [] as Integer[]
        new StringArraySerializer()                        | ['foo', 'bar'] as String[]
        new StringArraySerializer()                        | [] as String[]
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

    @Unroll
    def "serialized length #serializer.class.simpleName #value"() {
        when:
        def serialized = serializer.toByteArray(value)

        then:
        serialized.length == expectedLength

        where:
        serializer                                          | value | expectedLength
        KeyOrValueSerializer.STRING                         | ''    | 0
        KeyOrValueSerializer.STRING                         | 'a'   | 1
        KeyOrValueSerializer.STRING                         | 'foo' | 3
        KeyOrValueSerializer.STRING                         | 'föö' | 5
        new StringSerializer(StringSerializer.UTF_16BE)     | 'a'   | 2
        new StringSerializer(StringSerializer.UTF_16BE)     | 'foo' | 6
        new StringSerializer(StringSerializer.UTF_16BE)     | ''    | 0
        new StringSerializer(StringSerializer.UTF_16BE)     | 'föö' | 6
        new StringSerializer(Charset.forName('ISO-8859-1')) | 'a'   | 1
        new StringSerializer(Charset.forName('ISO-8859-1')) | 'foo' | 3
        new StringSerializer(Charset.forName('ISO-8859-1')) | 'föö' | 3
        KeyOrValueSerializer.INTEGER                        | 42    | 4
        KeyOrValueSerializer.BYTE                           | (byte) 42 | 1
    }
}