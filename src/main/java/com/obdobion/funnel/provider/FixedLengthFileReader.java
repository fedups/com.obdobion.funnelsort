package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>FixedLengthFileReader class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FixedLengthFileReader implements InputReader
{
    static final Logger logger = LoggerFactory.getLogger(FixedLengthFileReader.class);

    File                inputFile;
    RandomAccessFile    reader;

    /**
     * <p>Constructor for FixedLengthFileReader.</p>
     *
     * @param _inputFile a {@link java.io.File} object.
     * @param lineSeparator an array of byte.
     * @throws java.io.IOException if any.
     */
    public FixedLengthFileReader(final File _inputFile, final byte[] lineSeparator) throws IOException
    {
        open(_inputFile);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
    {
        reader.close();
        logger.debug("loaded " + inputFile.getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override
    public long length() throws IOException
    {
        return reader.length();
    }

    /** {@inheritDoc} */
    @Override
    public void open(final File _inputFile) throws IOException
    {
        inputFile = _inputFile;
        reader = new RandomAccessFile(_inputFile, "r");
    }

    /** {@inheritDoc} */
    @Override
    public long position() throws IOException
    {
        return reader.getFilePointer();
    }

    /** {@inheritDoc} */
    @Override
    public int read(final byte[] row) throws IOException
    {
        return reader.read(row);
    }
}
