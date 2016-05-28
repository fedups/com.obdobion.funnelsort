package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

/**
 * @author Chris DeGreef
 *
 */
public interface RandomAccessInputSource
{
    public void close () throws IOException, ParseException;

    public void open () throws IOException, ParseException;

    public int read (int originalInputFileIndex, byte[] originalBytes, long originalLocation, int originalSize)
        throws IOException;
}
