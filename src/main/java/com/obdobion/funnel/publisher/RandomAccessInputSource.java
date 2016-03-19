package com.obdobion.funnel.publisher;

import java.io.IOException;

/**
 * @author Chris DeGreef
 * 
 */
public interface RandomAccessInputSource
{
    public void close ()
        throws IOException;

    public void open ()
        throws IOException;

    public int read (
        int originalInputFileIndex,
        byte[] originalBytes,
        long originalLocation,
        int originalSize)
        throws IOException;
}
