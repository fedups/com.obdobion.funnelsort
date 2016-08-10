package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>FileSource class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FileSource implements RandomAccessInputSource
{
    static final private Logger logger = LoggerFactory.getLogger(FileSource.class);

    final FunnelContext         context;
    RandomAccessFile[]          raf;

    /**
     * <p>Constructor for FileSource.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public FileSource(final FunnelContext _context) throws ParseException, IOException
    {
        context = _context;
        raf = new RandomAccessFile[context.inputFileCount()];
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException, ParseException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i].close();
            logger.debug("releasing original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void open() throws IOException, ParseException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i] = new RandomAccessFile(context.getInputFile(i), "r");
            logger.debug("rereading original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    /** {@inheritDoc} */
    @Override
    public int read(
            final int originalInputFileIndex,
            final byte[] originalBytes,
            final long originalLocation,
            final int originalSize)
                    throws IOException
    {
        raf[originalInputFileIndex].seek(originalLocation);
        int readSize = originalSize;
        if (originalBytes.length < originalSize)
            readSize = originalBytes.length;
        return raf[originalInputFileIndex].read(originalBytes, 0, readSize);
    }
}
