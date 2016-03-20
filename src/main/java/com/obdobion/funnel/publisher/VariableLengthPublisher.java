package com.obdobion.funnel.publisher;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.App;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.parameters.DuplicateDisposition;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
abstract public class VariableLengthPublisher implements FunnelDataPublisher
{
    static final private Logger logger          = Logger.getLogger(VariableLengthPublisher.class);
    static final int            WriteBufferSize = 1 << 15;

    final FunnelContext         context;
    DataOutput                  writer;
    SourceProxyRecord           previousData;
    RandomAccessInputSource     originalFile;
    byte[]                      originalBytes;
    final byte[]                writeBuffer;
    final ByteBuffer            bb;
    private long                writeCount;
    private long                duplicateCount;

    public VariableLengthPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        this.context = _context;
        this.originalBytes = new byte[1024];

        initialize();

        writeBuffer = new byte[WriteBufferSize];
        bb = ByteBuffer.wrap(writeBuffer, 0, WriteBufferSize);

        logger.debug("write buffer size is " + WriteBufferSize + " bytes");
    }

    public void close () throws Exception
    {
        if (bb.position() != 0)
            flushWritesToDisk();
        originalFile.close();

        if (duplicateCount > 0)
            logger.info(Funnel.ByteFormatter.format(duplicateCount) + " duplicate rows");
        logger.info(Funnel.ByteFormatter.format(writeCount) + " rows written");
    }

    void flushWritesToDisk ()
        throws IOException
    {
        writer.write(bb.array(), 0, bb.position());
        bb.position(0);
    }

    public long getDuplicateCount ()
    {
        return duplicateCount;
    }

    public long getWriteCount ()
    {
        return writeCount;
    }

    private void initialize () throws ParseException, IOException
    {
        if (context.isCacheInput() || context.isSysin())
            originalFile = context.inputCache;
        else
            originalFile = new FileSource(context);

        try
        {
            openOutput(context);
        } catch (final IOException e)
        {
            App.abort(-1, e);
        }
        writeCount = duplicateCount = 0;
    }

    public void openInput () throws ParseException
    {
        try
        {
            originalFile.open();
        } catch (final IOException e)
        {
            App.abort(-1, e);
        }
    }

    abstract void openOutput (
        final FunnelContext _context)
        throws IOException,
        FileNotFoundException;

    public boolean publish (
        final SourceProxyRecord item,
        final long phase)
        throws Exception
    {
        /*
         * check to see if this item is in order, return false if not. The
         * originalRecordNumber is only used to order duplicates. At this point
         * it should not be used for comparisons since we want to make sure we
         * know a duplicate has been found.
         */
        item.originalRecordNumber = 0;

        int comparison = 0;
        if (previousData != null)
        {
            comparison = ((Comparable<SourceProxyRecord>) previousData).compareTo(item);
            if (comparison > 0)
                return false;
            /*
             * A duplicate record has been found.
             */
            if (comparison == 0)
            {
                duplicateCount++;
                if (DuplicateDisposition.FirstOnly == context.duplicateDisposition
                    || DuplicateDisposition.LastOnly == context.duplicateDisposition)
                    return true;
            }
        } else
        {
            /*
             * This is the first time publishing to this file. So lets see if
             * there is a header tucked away in the csv context area. We will
             * write that out first.
             */
            if (context.csv != null && context.csv.header && context.csv.headerContents != null)
            {
                write(context.csv.headerContents, 0, context.csv.headerContents.length);
                write(context.endOfRecordOutDelimiter, 0, context.endOfRecordOutDelimiter.length);
            }
        }
        /*
         * Get original data and write it to the output file.
         * 
         * Self-healing code. Expand the size of the work buffer if a row is
         * found to exceed the current buffer size. Get a bit extra so that we
         * aren't back to this trough too often. Garbage in memory is something
         * we want to stay away from.
         */
        if (item.originalSize > originalBytes.length)
        {
            originalBytes = new byte[item.originalSize + 1024];
        }

        originalFile.read(item.originalInputFileIndex, originalBytes, item.originalLocation, item.originalSize);
        write(originalBytes, 0, item.originalSize);
        write(context.endOfRecordOutDelimiter, 0, context.endOfRecordOutDelimiter.length);
        writeCount++;
        /*
         * Return the instance for reuse.
         */
        if (previousData != null)
            previousData.release();

        previousData = item;
        return true;
    }

    @Override
    public void reset () throws IOException, ParseException
    {
        initialize();
        if (previousData != null)
        {
            previousData.release();
            previousData = null;
        }
    }

    void write (
        final byte[] sourceBytes,
        final int off,
        final int len)
        throws IOException
    {
        final int sizeThisTime = len;

        if (sizeThisTime + bb.position() >= WriteBufferSize)
        {
            flushWritesToDisk();
        }

        bb.put(sourceBytes, off, len);
    }
}