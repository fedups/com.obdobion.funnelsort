package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

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
 * Read a file of byte arrays one row at a time
 *
 * @author Chris DeGreef
 *
 */
public class FixedLengthProvider implements FunnelDataProvider
{
    static final Logger logger = Logger.getLogger(FixedLengthProvider.class);

    final FunnelContext context;
    InputReader         reader;
    long                size;
    private long        recordNumber;
    byte[]              row;
    private long        unselectedCount;

    public FixedLengthProvider(final FunnelContext _context) throws IOException, ParseException
    {
        this.context = _context;
        initialize();
    }

    public long actualNumberOfRows ()
    {
        return maximumNumberOfRows();
    }

    public void attachTo (
            final FunnelItem item)
    {
        item.setProvider(this);
    }

    public void close () throws IOException, ParseException
    {
        if (reader == null)
            return;
        reader.close();
        reader = null;
    }

    private void initialize () throws IOException, ParseException
    {
        initializeReader();
        try
        {
            this.size = reader.length() / context.fixedRecordLength;
        } catch (final IOException e)
        {
            App.abort(-1, e);
        }
        this.row = new byte[context.fixedRecordLength];

        int optimalFunnelDepth = 2;
        long pow2 = size;

        /*
         * If the user specific a max rows expected then make sure to use that.
         * It might be the case that there are more than this single file being
         * sorted. And at this point we only know about the first one.
         */
        if (context.maximumNumberOfRows > 0)
            pow2 = context.maximumNumberOfRows;

        while (true)
        {
            if (pow2 < 2)
                break;
            pow2 /= 2;
            optimalFunnelDepth++;
        }
        if (context.depth > optimalFunnelDepth)
        {
            logger.debug("overriding power from " + context.depth + " to " + optimalFunnelDepth);
            context.depth = optimalFunnelDepth;
        }
    }

    protected void initializeReader () throws IOException, ParseException
    {
        if (context.isSysin())
            this.reader = new FixedLengthSysinReader(context);
        else if (context.isCacheInput())
            this.reader = new FixedLengthCacheReader(context);
        else
            this.reader =
                    new FixedLengthFileReader(context.getInputFile(context.inputFileIndex()),
                            context.endOfRecordDelimiter);
    }

    private void logStatistics (final int fileIndex) throws ParseException, IOException
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

    public long maximumNumberOfRows ()
    {
        return size;
    }

    public boolean next (final FunnelItem item, final long phase) throws IOException, ParseException
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

        boolean earlyEnd = false;
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
                        recordNumber = 0;
                        reader.close();
                        reader.open(context.getInputFile(context.inputFileIndex()));
                        continue;
                    }
                    break;
                }

                if (byteCount != -1 && byteCount != context.fixedRecordLength)
                {
                    logger.warn("Record truncated at EOF, bytes read = " + byteCount + ", bytes expected = "
                            + context.fixedRecordLength);
                    continue;
                }

                if (!isRowSelected(byteCount))
                {
                    recordNumber++;
                    continue;
                }

                preSelectionExtract(byteCount);

                if (context.stopIsTrue())
                {
                    earlyEnd = true;
                    /*
                     * The record number should be display as 1 relative while
                     * it is actually 0 relative. The EQU processing gets the 1
                     * relative version to use.
                     */
                    logger.debug("stopWhen triggered at row " + (recordNumber + 1));
                    break;
                }

                if (!context.whereIsTrue())
                {
                    recordNumber++;
                    unselectedCount++;
                    continue;
                }
                break;
            }
        } catch (final Exception e)
        {
            logger.fatal(e.getMessage(), e);
            throw new IOException(e.getMessage(), e);
        }
        if (byteCount == -1 || earlyEnd)
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

        KeyContext kContext = null;
        try
        {
            kContext = context.keyHelper.extractKey(row, recordNumber);
        } catch (final Exception e)
        {
            throw new IOException(e);
        }

        final SourceProxyRecord wrapped = SourceProxyRecord.getInstance(context);
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

        recordNumber++;
        item.setData(wrapped);
        return true;
    }

    boolean isRowSelected (@SuppressWarnings("unused") final int byteCount)
    {
        return true;
    }

    void preSelectionExtract (int byteCount) throws Exception
    {
        context.columnHelper.extract(context, row, recordNumber, byteCount);
    }

    public void reset () throws IOException, ParseException
    {
        initialize();
    }

    public void setMaximumNumberOfRows (
            final long max)
    {
        size = max;
    }
}