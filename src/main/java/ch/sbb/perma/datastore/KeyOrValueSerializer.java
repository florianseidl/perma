/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.datastore;

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
    IntegerSerializer INTEGER = new IntegerSerializer();
    LongSerializer LONG = new LongSerializer();
    JavaObjectSerializer JAVA_OBJECT = new JavaObjectSerializer();
    OptionalStringSerializer OPTIONAL_STRING = new OptionalStringSerializer();

    byte[] toByteArray(T object);
    T fromByteArray(byte[] bytes);
}
