/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.serializers;

/**
 * Serialize an Enum.
 *
 * @author u206123 (Florian Seidl)
 * @since 5.1, 2018.
 */
public class EnumSerializer<T extends Enum<T>> implements KeyOrValueSerializer<T> {
    private final Class<T> enumClass;

    public EnumSerializer(Class<T> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public byte[] toByteArray(T enumObject) {
        return STRING.toByteArray(enumObject.name());
    }

    @Override
    public T fromByteArray(byte[] bytes) {
        return Enum.valueOf(enumClass, STRING.fromByteArray(bytes));
    }
}
