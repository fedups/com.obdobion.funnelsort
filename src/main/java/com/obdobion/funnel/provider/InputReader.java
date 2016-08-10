package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

/**
 * <p>InputReader interface.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface InputReader
{
    /**
     * <p>close.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    void close () throws IOException, ParseException;

    /**
     * <p>length.</p>
     *
     * @return a long.
     * @throws java.io.IOException if any.
     */
    long length () throws IOException;

    /**
     * <p>open.</p>
     *
     * @param _inputFile a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    void open (File _inputFile) throws IOException, ParseException;

    /**
     * <p>position.</p>
     *
     * @return a long.
     * @throws java.io.IOException if any.
     */
    long position () throws IOException;

    /**
     * <p>read.</p>
     *
     * @param row an array of byte.
     * @return a int.
     * @throws java.io.IOException if any.
     */
    int read (final byte[] row) throws IOException;
}
