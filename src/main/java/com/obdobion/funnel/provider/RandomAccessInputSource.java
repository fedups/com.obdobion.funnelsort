package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

/**
 * <p>RandomAccessInputSource interface.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface RandomAccessInputSource
{
    /**
     * <p>close.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void close () throws IOException, ParseException;

    /**
     * <p>open.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void open () throws IOException, ParseException;

    /**
     * <p>read.</p>
     *
     * @param originalInputFileIndex a int.
     * @param originalBytes an array of byte.
     * @param originalLocation a long.
     * @param originalSize a int.
     * @return a int.
     * @throws java.io.IOException if any.
     */
    public int read (int originalInputFileIndex, byte[] originalBytes, long originalLocation, int originalSize)
        throws IOException;
}
