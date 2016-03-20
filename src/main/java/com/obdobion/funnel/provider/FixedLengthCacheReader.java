package com.obdobion.funnel.provider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthCacheReader implements InputReader
{
    static final Logger logger = Logger.getLogger(FixedLengthCacheReader.class);

    final FunnelContext context;
    long                currentPosition;

    public FixedLengthCacheReader(final FunnelContext _context) throws IOException, ParseException
    {
        this.context = _context;
        loadDataToCache();
        currentPosition = 0;
    }

    public void close ()
        throws IOException
    {
        // intentionally empty
    }

    public long length ()
        throws IOException
    {
        return context.inputCache.length();
    }

    void loadDataToCache () throws IOException, ParseException
    {
        final FileInputStream inputStream = new FileInputStream(context.getInputFile(context.inputFileIndex()));
        context.inputCache = new InputCache(context, inputStream);
        inputStream.close();
        logger.debug("loaded " + context.getInputFile(context.inputFileIndex()).getAbsolutePath());
    }

    public void open (final File inputFile)
    {
        // intentionally empty
    }

    public long position ()
        throws IOException
    {
        return context.inputCache.position();
    }

    public int read (
        final byte[] row)
        throws IOException
    {
        if (context.inputCache.eof())
            return -1;

        final int count = context.inputCache.read(context.inputFileIndex(), row, currentPosition, row.length);
        currentPosition += count;
        return count;
    }

    public void reset ()
    {
        // Intentionally empty
    }
}
