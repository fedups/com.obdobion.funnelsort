package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

/**
 * @author Chris DeGreef
 *
 */
public interface InputReader
{
    void close () throws IOException, ParseException;

    long length () throws IOException;

    void open (File _inputFile) throws IOException, ParseException;

    long position () throws IOException;

    int read (final byte[] row) throws IOException;
}