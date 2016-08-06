package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * Read a file of ascii strings one line at a time
 *
 * @author Chris DeGreef
 *
 */
public class VariableLengthProvider extends AbstractProvider
{
    private static final int MAX_VARIABLE_LENGTH_RECORD_SIZE = 1 << 13;

    public VariableLengthProvider(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
    }

    @Override
    public long actualNumberOfRows()
    {
        return getContinuousRecordNumber();
    }

    void assignReaderInstance() throws IOException, ParseException
    {
        if (context.isSysin())
            reader = new VariableLengthSysinReader(context);
        else if (context.isCacheInput())
            reader = new VariableLengthCacheReader(context);
        else
            reader = new VariableLengthFileReader(context);
    }

    @Override
    void initialize() throws IOException, ParseException
    {
        row = new byte[MAX_VARIABLE_LENGTH_RECORD_SIZE];

        logger.debug(row.length + " byte array size for rows.  This is an arbitrary upper limit.");

        int optimalFunnelDepth = 2;
        long pow2 = context.getMaximumNumberOfRows();
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

        setThisFileRecordNumber(unselectedCount = 0);

        assignReaderInstance();
    }

    @Override
    public long maximumNumberOfRows()
    {
        return context.getMaximumNumberOfRows();
    }

}