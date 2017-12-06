/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

/**
 * Serialize a key or a value.
 * <p>
 *     Extend this to implement a custom serializer.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public interface KeyOrValueSerializer<T> {
    StringSerializer STRING = new StringSerializer();
    ShortStringSerializer SHORT_STRING = new ShortStringSerializer();
    IntegerSerializer INTEGER = new IntegerSerializer();
    LongSerializer LONG = new LongSerializer();
    ByteSerializer BYTE = new ByteSerializer();
    ShortSerializer SHORT = new ShortSerializer();
    DoubleSerializer DOUBLE = new DoubleSerializer();
    FloatSerializer FLOAT = new FloatSerializer();
    BigDecimalSerializer BIG_DECIMAL = new BigDecimalSerializer();
    BigIntegerSerializer BIG_INTEGER = new BigIntegerSerializer();
    CharSerializer CHAR = new CharSerializer();
    JavaObjectSerializer JAVA_OBJECT = new JavaObjectSerializer();
    OptionalStringSerializer OPTIONAL_STRING = new OptionalStringSerializer();
    LocalDateSerializer LOCAL_DATE = new LocalDateSerializer();
    LocalTimeSerializer LOCAL_TIME = new LocalTimeSerializer();
    LocalDateTimeSerializer LOCAL_DATE_TIME = new LocalDateTimeSerializer();
    ZonedDateTimeSerializer ZONED_DATE_TIME = new ZonedDateTimeSerializer();
    OffsetDateTimeSerializer OFFSET_DATE_TIME = new OffsetDateTimeSerializer();
    DateSerializer DATE = new DateSerializer();

    byte[] toByteArray(T object);
    T fromByteArray(byte[] bytes);
}
