package com.obdobion.funnel.publisher;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.App;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.aggregation.Aggregate;
import com.obdobion.funnel.columns.ColumnWriter;
import com.obdobion.funnel.parameters.DuplicateDisposition;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.provider.FileSource;
import com.obdobion.funnel.provider.RandomAccessInputSource;
import com.obdobion.funnel.segment.SourceProxyRecord;

abstract public class AbstractPublisher implements FunnelDataPublisher, ColumnWriter
{
    static final private Logger logger          = LoggerFactory.getLogger(AbstractPublisher.class);

    static final int            WriteBufferSize = 1 << 15;

    FunnelContext               context;
    DataOutput                  writer;
    SourceProxyRecord           previousItem;
    byte[]                      previousOriginalBytes;
    RandomAccessInputSource     originalFile;
    byte[]                      originalBytes;
    byte[]                      writeBuffer;
    ByteBuffer                  bb;
    long                        writeCount;
    long                        duplicateCount;

    public AbstractPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        this.context = _context;

        initialize();

        writeBuffer = new byte[WriteBufferSize];
        bb = ByteBuffer.wrap(writeBuffer, 0, WriteBufferSize);

        logger.debug("write buffer size is " + WriteBufferSize + " bytes");
    }

    public void close () throws Exception
    {
        if (context.isAggregating() && previousItem != null)
        {
            /*
             * Write last aggregation to disk
             */
            formatOutputAndWrite(previousItem, previousOriginalBytes);
        }
        if (bb.position() != 0)
            flushWritesToDisk();
        originalFile.close();

        context.outputCounters(duplicateCount, writeCount);

        if (duplicateCount > 0)
            logger.debug(Funnel.ByteFormatter.format(duplicateCount) + " duplicate rows");
        logger.debug(Funnel.ByteFormatter.format(writeCount) + " rows written");
    }

    void flushWritesToDisk ()
        throws IOException
    {
        writer.write(bb.array(), 0, bb.position());
        bb.position(0);
    }

    void formatOutputAndWrite (final SourceProxyRecord item, final byte[] rawData)
        throws IOException, Exception
    {
        writeCount++;
        /*
         * Prepare the aggregations for the next set of data.
         */
        Aggregate.reset(context);
    }

    public long getDuplicateCount ()
    {
        return duplicateCount;
    }

    @Override
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

    void loadOriginalBytes (final int originalFileNumber, final SourceProxyRecord item)
        throws IOException
    {
        originalFile.read(originalFileNumber, originalBytes, item.originalLocation, item.originalSize);
    }

    @Override
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

    abstract void openOutput (final FunnelContext _context) throws IOException, FileNotFoundException;

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
         * The same goes for the original file number. But it is important not
         * to loose this information because it is needed to get the original
         * data.
         */
        final int originalFileNumber = item.originalInputFileIndex;
        item.originalInputFileIndex = 0;

        int comparison = 0;

        loadOriginalBytes(originalFileNumber, item);
        item.getFunnelContext().columnHelper
                .loadColumnsFromBytes(originalBytes, item.originalSize, item.originalRecordNumber);

        if (previousItem != null)
        {
            comparison = ((Comparable<SourceProxyRecord>) previousItem).compareTo(item);
            if (comparison > 0)
                return false;
            /*
             * A duplicate record has been found.
             */
            if (comparison == 0)
            {
                if (context.isAggregating())
                {
                    /*
                     * Rather than write anything during an aggregation run we
                     * just aggregate until the key changes.
                     */
                    Aggregate.aggregate(context);
                    return true;
                }
                duplicateCount++;
                if (DuplicateDisposition.FirstOnly == context.duplicateDisposition
                    || DuplicateDisposition.LastOnly == context.duplicateDisposition)
                    /*
                     * Since the file is sorted so that the duplicate we want to
                     * retain is first, and because it was not a duplicate until
                     * after it has been seen, we can easily ignore all
                     * duplicates.
                     */
                    return true;
            }
        } else
        {
            publishHeader();
            if (context.isAggregating())
            {
                /*
                 * Never write the first record when aggregating. Wait until the
                 * key changes.
                 */
                Aggregate.aggregate(context);
                previousOriginalBytes = Arrays.copyOf(originalBytes, item.originalSize);
                previousItem = item;
                return true;
            }
        }

        if (context.isAggregating())
        {
            /*
             * We must reload the previous values into the columns since the new
             * set of records has already started.
             */
            item.getFunnelContext().columnHelper
                    .loadColumnsFromBytes(previousOriginalBytes, previousItem.originalSize, previousItem.originalRecordNumber);
            formatOutputAndWrite(previousItem, previousOriginalBytes);
            /*
             * Now reload the newest record into the columns for processing.
             */
            item.getFunnelContext().columnHelper
                    .loadColumnsFromBytes(originalBytes, item.originalSize, item.originalRecordNumber);
            Aggregate.aggregate(context);

        } else
            formatOutputAndWrite(item, originalBytes);
        /*
         * Return the instance for reuse.
         */
        if (previousItem != null)
            previousItem.release();

        previousItem = item;
        previousOriginalBytes = Arrays.copyOf(originalBytes, item.originalSize);
        return true;
    }

    abstract void publishHeader () throws IOException;

    public void reset () throws IOException, ParseException
    {
        initialize();
        if (previousItem != null)
        {
            previousItem.release();
            previousItem = null;
        }
    }

    @Override
    public void write (final byte[] sourceBytes, final int offset, final int length) throws IOException
    {
        if (length + bb.position() >= WriteBufferSize)
        {
            flushWritesToDisk();
        }
        bb.put(sourceBytes, offset, length);
    }
}
