/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package ch.sbb.perma.file;

import java.io.IOException;

@FunctionalInterface
public interface IOFunction<T,R> {
    R apply(T stream) throws IOException;
}
