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
public class VariableLengthCacheReader implements InputReader
{
    static final Logger logger = LoggerFactory.getLogger(VariableLengthCacheReader.class);

    final FunnelContext context;

    public VariableLengthCacheReader(final FunnelContext _context) throws IOException, ParseException
    {
        this.context = _context;
        logger.debug("variable length cache reader activated");
        loadDataToCache();
    }

    public void close ()
        throws IOException
    {
        // intentionally. Cached input is opened and closed when the instance is
        // made.
    }

    public long length ()
        throws IOException
    {
        return context.inputCache.length();
    }

    void loadDataToCache () throws IOException, ParseException
    {
        final FileInputStream inputStream = new FileInputStream(context.getInputFile(context.inputFileIndex()));
        context.inputCache = new VariableLengthInputCache(context, inputStream);
        inputStream.close();
        logger.debug("loaded " + context.getInputFile(context.inputFileIndex()).getAbsolutePath());
    }

    public void open (final File inputFile) throws IOException
    {
        throw new IOException("the cacheInput option is not allowed with multiple input files");
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
        /*
         * Clear the row before reading.
         */
        for (int b = 0; b < row.length; b++)
            row[b] = 0x00;

        int rowNextPointer = 0;
        int sepNextPointer = 0;
        while (!context.inputCache.eof())
        {
            final byte b = context.inputCache.readNextByte();
            /*
             * Then see of this character is the next expected character in the
             * end of row sequence.
             */
            if (b == context.endOfRecordDelimiterIn[sepNextPointer])
            {
                if (sepNextPointer == context.endOfRecordDelimiterIn.length - 1)
                {
                    return rowNextPointer;
                }
                sepNextPointer++;
                continue;
            }
            /*
             * Something that started to look like the end of row turned out not
             * to be. So give it to the caller.
             */
            if (sepNextPointer > 0)
            {
                for (int sp = 0; sp < sepNextPointer; sp++)
                    row[rowNextPointer++] = context.endOfRecordDelimiterIn[sp];
                sepNextPointer = 0;
            }
            /*
             * transfer the byte to the row.
             */
            row[rowNextPointer++] = b;
        }
        if (context.inputCache.eof())
        {
            /*
             * end of file without end of record, this is ok.
             */
            if (rowNextPointer == 0)
                return -1;
        }
        /*
         * We have hit the end of the file and did not find an end of line for
         * the last bytes we did find. Return the row for what is there.
         */
        logger.warn("assuming a line terminator at end of file where " + rowNextPointer
            + " unterminated bytes were found");
        return rowNextPointer;
    }
}
