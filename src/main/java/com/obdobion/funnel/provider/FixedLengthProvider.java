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
    private long        position;
    private long        recordNumber;
    byte[]              recordContents;
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
        this.recordContents = new byte[context.fixedRecordLength];

        int optimalFunnelDepth = 2;
        long pow2 = size;
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

        int bcount = 0;
        try
        {
            while (true)
            {
                bcount = reader.read(recordContents);

                if (bcount == -1)
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

                if (bcount != -1 && bcount != context.fixedRecordLength)
                    throw new IOException("Record truncated at EOF, bytes read = " + bcount + ", bytes expected = "
                        + context.fixedRecordLength);

                context.columnHelper.extract(recordContents, recordNumber, bcount);
                if (context.columnHelper.whereIsTrue())
                {
                    break;
                }
                recordNumber++;
                unselectedCount++;
                continue;
            }
        } catch (final Exception e)
        {
            App.abort(-1, e);
        }
        if (bcount == -1)
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
            kContext = context.keyHelper.extractKey(recordContents, recordNumber);
        } catch (final Exception e)
        {
            throw new IOException(e);
        }

        final SourceProxyRecord wrapped = SourceProxyRecord.getInstance();
        wrapped.originalInputFileIndex = context.inputFileIndex();

        wrapped.size = kContext.keyLength;
        wrapped.sortKey = kContext.key;
        wrapped.originalSize = bcount;
        wrapped.originalLocation = position;

        if (DuplicateDisposition.LastOnly == context.duplicateDisposition
            || DuplicateDisposition.Reverse == context.duplicateDisposition)
            wrapped.originalRecordNumber = -recordNumber;
        else
            wrapped.originalRecordNumber = recordNumber;

        position += wrapped.originalSize;
        recordNumber++;
        item.setData(wrapped);
        return true;
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