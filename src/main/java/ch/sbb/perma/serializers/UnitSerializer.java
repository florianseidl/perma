/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import org.javatuples.Unit;

/**
 * Serializer and deserializer for javatuples Unit. Can be used to wrap nullable values.
 * <p>
 *     Requires a serializer for the value. It may not seriaize non-null values to null.
 * </p>
 *
 * @author u206123 (Florian Seidl)
 * @since 3.1, 2017.
 */
public class UnitSerializer<A> extends TupleSerializer<Unit<A>> {

    public UnitSerializer(KeyOrValueSerializer<A> serializer) {
        super(serializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    Unit<A> createFrom(Object[] values) {
        return new Unit(values[0]);
    }
}
