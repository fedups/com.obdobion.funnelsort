package com.obdobion.funnel.provider;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.App;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.parameters.DuplicateDisposition;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * Read a file of ascii strings one line at a time
 *
 * @author Chris DeGreef
 *
 */
public class VariableLengthProvider implements FunnelDataProvider
{
    private static final int MAX_VARIABLE_LENGTH_RECORD_SIZE = 1 << 13;

    static final Logger      logger                          = Logger.getLogger(VariableLengthProvider.class);

    final FunnelContext      context;
    InputReader              reader;
    long                     recordNumber;
    final byte               row[];
    int                      unselectedCount;

    public VariableLengthProvider(
            final FunnelContext _context)
            throws IOException
    {
        this.context = _context;
        /*
         * This is really to handle test cases. It should never be null.
         */
        if (_context == null)
        {
            row = null;
            return;
        }

        initialize();

        this.row = new byte[MAX_VARIABLE_LENGTH_RECORD_SIZE];

        logger.debug(this.row.length + " byte array size for rows.  This is an arbitrary upper limit.");

        int optimalFunnelDepth = 2;
        long pow2 = _context.maximumNumberOfRows;
        while (true)
        {
            if (pow2 < 2)
                break;
            pow2 /= 2;
            optimalFunnelDepth++;
        }
        if (_context.depth > optimalFunnelDepth)
        {
            logger.debug("overriding power from " + _context.depth + " to " + optimalFunnelDepth);

            _context.depth = optimalFunnelDepth;
        }

    }

    private void initialize ()
        throws IOException
    {
        recordNumber = unselectedCount = 0;

        if (context.isSysin())
            this.reader = new VariableLengthSysinReader(context);
        else if (context.isCacheInput())
            this.reader = new VariableLengthCacheReader(context);
        else
            this.reader = new VariableLengthFileReader(context);
    }

    public long actualNumberOfRows ()
    {
        return recordNumber;
    }

    public void attachTo (
        final FunnelItem item)
    {
        item.setProvider(this);
    }

    public void close ()
        throws IOException
    {
        if (reader == null)
            return;
        reader.close();
        reader = null;
    }

    /**
     * @param byteCount
     * @return
     */
    boolean isRowSelected (
        final int byteCount)
    {
        return true;
    }

    public long maximumNumberOfRows ()
    {
        return context.maximumNumberOfRows;
    }

    public boolean next (
        final FunnelItem item,
        final long phase)
        throws IOException
    {
        /*
         * Only return 1 row per phase per item.
         */
        if (item.getPhase() == phase || reader == null)
        {
            item.setEndOfData(true);
            return false;
        }
        item.setPhase(phase);

        int byteCount = 0;
        long startPosition = 0;
        try
        {
            while (true)
            {
                startPosition = reader.position();
                byteCount = reader.read(row);

                if (byteCount == -1)
                {
                    /*
                     * See if there are more files to be read.
                     */
                    if (context.startNextInput())
                    {
                        logStatistics(context.inputFileIndex() - 1);
                        recordNumber = unselectedCount = 0;
                        reader.close();
                        reader.open(context.getInputFile(context.inputFileIndex()));
                        continue;
                    }
                    break;
                }

                context.columnHelper.extract(row, recordNumber, byteCount);
                if (!isRowSelected(byteCount))
                {
                    recordNumber++;
                    continue;
                }

                if (!context.columnHelper.whereIsTrue())
                {
                    recordNumber++;
                    unselectedCount++;
                    continue;
                }
                break;
            }

        } catch (final Exception e)
        {
            App.abort(-1, e);
        }
        if (byteCount == -1)
        {
            item.setEndOfData(true);
            try
            {
                logStatistics(context.inputFileIndex());
                close();
            } catch (final IOException e)
            {
                e.printStackTrace();
            }
            return false;
        }
        /*
         * Putting this incrementer here causes the record number to be 1
         * relative. Move it to the end of this method if we want it to be 0
         * relative.
         */
        recordNumber++;

        final KeyContext kContext = postReadKeyProcessing(byteCount);

        final SourceProxyRecord wrapped = SourceProxyRecord.getInstance();
        wrapped.originalInputFileIndex = context.inputFileIndex();

        wrapped.size = kContext.keyLength;
        wrapped.sortKey = kContext.key;
        wrapped.originalSize = byteCount;
        wrapped.originalLocation = startPosition;

        if (DuplicateDisposition.LastOnly == context.duplicateDisposition
            || DuplicateDisposition.Reverse == context.duplicateDisposition)
            wrapped.originalRecordNumber = -recordNumber;
        else
            wrapped.originalRecordNumber = recordNumber;

        item.setData(wrapped);
        return true;
    }

    private void logStatistics (final int fileIndex)
    {
        final StringBuilder sb = new StringBuilder();

        sb.append(Funnel.ByteFormatter.format(recordNumber));
        sb.append(" rows obtained from ");
        if (context.isSysin())
            sb.append("SYSIN");
        else
            sb.append(context.getInputFile(fileIndex).getName());
        if (unselectedCount > 0)
        {
            sb.append(", ");
            sb.append(Funnel.ByteFormatter.format(unselectedCount));
            sb.append(" filtered out by where clause");
        }
        logger.info(sb.toString());
    }

    /**
     * @param byteCount
     * @return
     * @throws IOException
     */
    KeyContext postReadKeyProcessing (
        final int byteCount)
        throws IOException
    {
        KeyContext kContext = null;
        try
        {
            kContext = context.keyHelper.extractKey(row, recordNumber);
        } catch (final Exception e)
        {
            throw new IOException(e);
        }
        return kContext;
    }

    public void reset ()
        throws IOException
    {
        initialize();
    }
}