package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthFileReader implements InputReader
{
    static final Logger logger = LoggerFactory.getLogger(FixedLengthFileReader.class);

    File                inputFile;
    RandomAccessFile    reader;

    /**
     * @param _inputFile
     * @param lineSeparator
     * @throws IOException
     */
    public FixedLengthFileReader(
            final File _inputFile, final byte[] lineSeparator)
            throws IOException
    {
        open(_inputFile);
    }

    public void close ()
        throws IOException
    {
        reader.close();
        logger.debug("loaded " + inputFile.getAbsolutePath());
    }

    public long length ()
        throws IOException
    {
        return reader.length();
    }

    public void open (
        final File _inputFile)
        throws IOException
    {
        this.inputFile = _inputFile;
        this.reader = new RandomAccessFile(_inputFile, "r");
    }

    public long position ()
        throws IOException
    {
        return reader.getFilePointer();
    }

    public int read (
        final byte[] row)
        throws IOException
    {
        return reader.read(row);
    }
}
