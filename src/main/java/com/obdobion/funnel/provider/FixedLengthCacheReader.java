package com.obdobion.funnel.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>FixedLengthCacheReader class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FixedLengthCacheReader implements InputReader
{
    static final Logger logger = LoggerFactory.getLogger(FixedLengthCacheReader.class);

    final FunnelContext context;
    long                currentPosition;

    /**
     * <p>Constructor for FixedLengthCacheReader.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public FixedLengthCacheReader(final FunnelContext _context) throws IOException, ParseException
    {
        context = _context;
        logger.debug("fixed length cache provider activated");
        loadDataToCache();
        currentPosition = 0;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

    /** {@inheritDoc} */
    @Override
    public long length() throws IOException
    {
        return context.inputCache.length();
    }

    void loadDataToCache() throws IOException, ParseException
    {
        try (final FileInputStream inputStream = new FileInputStream(context.getInputFile(context.inputFileIndex())))
        {
            context.inputCache = new FixedLengthInputCache(context, inputStream);
            logger.debug("loaded " + context.getInputFile(context.inputFileIndex()).getAbsolutePath());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void open(final File inputFile)
    {
        // intentionally empty
    }

    /** {@inheritDoc} */
    @Override
    public long position() throws IOException
    {
        return context.inputCache.position();
    }

    /** {@inheritDoc} */
    @Override
    public int read(final byte[] row) throws IOException
    {
        if (context.inputCache.eof())
            return -1;

        final int count = context.inputCache.read(context.inputFileIndex(), row, currentPosition, row.length);
        currentPosition += count;
        return count;
    }

    /**
     * <p>reset.</p>
     */
    public void reset()
    {
        // Intentionally empty
    }
}
