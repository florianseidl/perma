/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2017.
 */

package ch.sbb.perma.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Serialize and deserialize using java serialization.
 * <p>
 * Poor performance, use with care. Better write your own serializer.
 *
 * @author u206123 (Florian Seidl)
 * @since 1.0, 2017.
 */
public class JavaObjectSerializer<T extends Serializable> implements KeyOrValueSerializer<T> {
    @Override
    public byte[] toByteArray(T object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            oos.flush();
            return bos.toByteArray();
        }
        catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T fromByteArray(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        }
        catch (IOException | ClassNotFoundException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
