package com.obdobion.funnel.publisher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class FixedLengthFilePublisher extends FixedLengthPublisher
{
    static final private Logger logger = Logger.getLogger(FixedLengthFilePublisher.class);

    public FixedLengthFilePublisher(final FunnelContext _context)
    {
        super(_context);
    }

    @Override
    public void close () throws IOException
    {
        super.close();

        ((RandomAccessFile) writer).close();

        if (context.outputFile.delete())
            logger.debug("deleted " + context.outputFile.getAbsolutePath());

        if (!sortedTempFile.renameTo(context.outputFile))
            throw new IOException("failed to rename " + sortedTempFile.getAbsolutePath() + " to "
                    + context.outputFile.getAbsolutePath());

        logger.debug("renamed " + sortedTempFile.getAbsolutePath() + " to " + context.outputFile.getAbsolutePath());
    }

    @Override
    void openOutput (final FunnelContext context2) throws IOException
    {
        sortedTempFile = File.createTempFile("Sorted.", ".tmp", context.outputFile.getParentFile());
        this.writer = new RandomAccessFile(sortedTempFile, "rw");

        logger.debug("writing " + sortedTempFile.getAbsolutePath());
    }
}