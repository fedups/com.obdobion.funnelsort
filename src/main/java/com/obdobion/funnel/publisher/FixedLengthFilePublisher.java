package com.obdobion.funnel.publisher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthFilePublisher extends FixedLengthPublisher
{
    static final private Logger logger = LoggerFactory.getLogger(FixedLengthFilePublisher.class);

    File                        sortedTempFile;

    public FixedLengthFilePublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        logger.debug("fixed length file publisher activated");
    }

    @Override
    public void close() throws Exception
    {
        super.close();

        ((RandomAccessFile) writer).close();

        if (context.getOutputFile().delete())
            logger.debug("deleted {}", context.getOutputFile().getAbsolutePath());

        if (!sortedTempFile.renameTo(context.getOutputFile()))
            throw new IOException("failed to rename "
                    + sortedTempFile.getAbsolutePath()
                    + " to "
                    + context.getOutputFile().getAbsolutePath());

        logger.debug("renamed {} to {}", sortedTempFile.getAbsolutePath(), context.getOutputFile().getAbsolutePath());
    }

    /**
     * @param context2
     */
    @Override
    void openOutput(final FunnelContext context2) throws IOException
    {
        sortedTempFile = File
                .createTempFile("Sorted.", ".tmp", context.getOutputFile().getAbsoluteFile().getParentFile());
        sortedTempFile.deleteOnExit();
        writer = new RandomAccessFile(sortedTempFile, "rw");

        logger.debug("writing {}", sortedTempFile.getAbsolutePath());
    }
}