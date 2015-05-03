package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;

/**
 * @author Chris DeGreef
 * 
 */
public interface InputReader
{
    void close ()
        throws IOException;

    long length ()
        throws IOException;

    void open (File _inputFile)
        throws IOException;

    long position ()
        throws IOException;

    int read (final byte[] row)
        throws IOException;
}