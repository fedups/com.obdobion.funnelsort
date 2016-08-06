package com.obdobion.funnel.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthCacheReader implements InputReader
{
    static final Logger logger = LoggerFactory.getLogger(FixedLengthCacheReader.class);

    final FunnelContext context;
    long                currentPosition;

    public FixedLengthCacheReader(final FunnelContext _context) throws IOException, ParseException
    {
        context = _context;
        logger.debug("fixed length cache provider activated");
        loadDataToCache();
        currentPosition = 0;
    }

    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

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

    /**
     * @param inputFile
     */
    @Override
    public void open(final File inputFile)
    {
        // intentionally empty
    }

    @Override
    public long position() throws IOException
    {
        return context.inputCache.position();
    }

    @Override
    public int read(final byte[] row) throws IOException
    {
        if (context.inputCache.eof())
            return -1;

        final int count = context.inputCache.read(context.inputFileIndex(), row, currentPosition, row.length);
        currentPosition += count;
        return count;
    }

    public void reset()
    {
        // Intentionally empty
    }
}
