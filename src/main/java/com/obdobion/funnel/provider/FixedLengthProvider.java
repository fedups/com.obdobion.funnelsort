package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.App;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * Read a file of byte arrays one row at a time
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FixedLengthProvider extends AbstractProvider
{
    long size;

    /**
     * <p>Constructor for FixedLengthProvider.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public FixedLengthProvider(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        logger.debug("fixed length file provider activated");
    }

    /** {@inheritDoc} */
    @Override
    public long actualNumberOfRows()
    {
        return maximumNumberOfRows();
    }

    @Override
    void initialize() throws IOException, ParseException
    {
        initializeReader();
        try
        {
            size = reader.length() / context.getFixedRecordLengthIn();
        } catch (final IOException e)
        {
            App.abort(-1, e);
        }
        row = new byte[context.getFixedRecordLengthIn()];

        int optimalFunnelDepth = 2;
        long pow2 = size;

        /*
         * If the user specific a max rows expected then make sure to use that.
         * It might be the case that there are more than this single file being
         * sorted. And at this point we only know about the first one.
         */
        if (context.getMaximumNumberOfRows() > 0)
            pow2 = context.getMaximumNumberOfRows();

        while (true)
        {
            if (pow2 < 2)
                break;
            pow2 /= 2;
            optimalFunnelDepth++;
        }
        if (context.getDepth() > optimalFunnelDepth)
        {
            logger.debug("overriding power from " + context.getDepth() + " to " + optimalFunnelDepth);
            context.setDepth(optimalFunnelDepth);
        }
    }

    /**
     * <p>initializeReader.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    protected void initializeReader() throws IOException, ParseException
    {
        if (context.isSysin())
            reader = new FixedLengthSysinReader(context);
        else if (context.isCacheInput())
            reader = new FixedLengthCacheReader(context);
        else
            reader = new FixedLengthFileReader(context
                    .getInputFile(context.inputFileIndex()), context.getEndOfRecordDelimiterIn());
    }

    /** {@inheritDoc} */
    @Override
    public long maximumNumberOfRows()
    {
        return size;
    }

    @Override
    boolean recordLengthOK(final int byteCount)
    {
        if (byteCount != -1 && byteCount != context.getFixedRecordLengthIn())
        {
            logger.warn("Record truncated at EOF, bytes read = "
                    + byteCount
                    + ", bytes expected = "
                    + context.getFixedRecordLengthIn());
            return false;
        }
        return true;
    }

    /**
     * <p>setMaximumNumberOfRows.</p>
     *
     * @param max a long.
     */
    public void setMaximumNumberOfRows(final long max)
    {
        size = max;
    }
}
