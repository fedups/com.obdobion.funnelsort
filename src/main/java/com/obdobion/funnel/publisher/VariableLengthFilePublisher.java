package com.obdobion.funnel.publisher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthFilePublisher extends VariableLengthPublisher
{
    static final private Logger logger = Logger.getLogger(VariableLengthFilePublisher.class);

    File                        sortedTempFile;

    public VariableLengthFilePublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        logger.debug("variable length file publisher activated");
    }

    @Override
    public void close () throws Exception
    {
        super.close();

        ((RandomAccessFile) writer).close();

        if (context.outputFile.delete())
            logger.trace("deleted " + context.outputFile.getAbsolutePath());

        if (!sortedTempFile.renameTo(context.outputFile))
            throw new IOException("failed to rename " + sortedTempFile.getAbsolutePath() + " to "
                + context.outputFile.getAbsolutePath());

        logger.trace("renamed " + sortedTempFile.getAbsolutePath() + " to " + context.outputFile.getAbsolutePath());

    }

    @Override
    void openOutput (final FunnelContext _context) throws IOException, FileNotFoundException
    {
        sortedTempFile = File.createTempFile("Sorted.", ".tmp", _context.outputFile.getParentFile());
        this.writer = new RandomAccessFile(sortedTempFile, "rw");

        logger.debug("writing " + sortedTempFile.getAbsolutePath());
    }
}