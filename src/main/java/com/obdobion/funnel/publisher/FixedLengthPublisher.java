package com.obdobion.funnel.publisher;

import java.io.DataOutput;
import java.io.File;
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
abstract public class FixedLengthPublisher implements FunnelDataPublisher
{
    static final private Logger logger          = Logger.getLogger(FixedLengthPublisher.class);
    static final int            WriteBufferSize = 1 << 15;

    final FunnelContext         context;
    DataOutput                  writer;
    SourceProxyRecord           previousData;
    File                        sortedTempFile;
    RandomAccessInputSource     originalFile;
    final byte[]                originalBytes;
    final byte[]                writeBuffer;
    final ByteBuffer            bb;
    private long                writeCount;
    private long                duplicateCount;

    public FixedLengthPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        this.context = _context;
        this.originalBytes = new byte[_context.fixedRecordLength];

        initialize();

        writeBuffer = new byte[WriteBufferSize];
        bb = ByteBuffer.wrap(writeBuffer, 0, WriteBufferSize);

        logger.debug("write buffer size is " + WriteBufferSize + " bytes");
    }

    public void close () throws IOException, ParseException
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
        FunnelContext _context)
        throws IOException;

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
        /*
         * check to see if this item is in order, return false if not.
         */
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
        }
        /*
         * Get original data and write it to the output file.
         */
        originalFile.read(item.originalInputFileIndex, originalBytes, item.originalLocation, item.originalSize);
        write(originalBytes);
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
            previousData.release();
    }

    void write (
        final byte[] _originalBytes)
        throws IOException
    {
        final int sizeThisTime = _originalBytes.length;

        if (sizeThisTime + bb.position() >= WriteBufferSize)
        {
            flushWritesToDisk();
        }
        bb.put(_originalBytes);
    }
}