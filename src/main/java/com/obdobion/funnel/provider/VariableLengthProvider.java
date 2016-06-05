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

    public long actualNumberOfRows ()
    {
        return recordNumber;
    }

    void assignReaderInstance () throws IOException, ParseException
    {
        if (context.isSysin())
            this.reader = new VariableLengthSysinReader(context);
        else if (context.isCacheInput())
            this.reader = new VariableLengthCacheReader(context);
        else
            this.reader = new VariableLengthFileReader(context);
    }

    @Override
    void initialize () throws IOException, ParseException
    {
        this.row = new byte[MAX_VARIABLE_LENGTH_RECORD_SIZE];

        logger.debug(this.row.length + " byte array size for rows.  This is an arbitrary upper limit.");

        int optimalFunnelDepth = 2;
        long pow2 = context.maximumNumberOfRows;
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

        recordNumber = unselectedCount = 0;

        assignReaderInstance();
    }

    public long maximumNumberOfRows ()
    {
        return context.maximumNumberOfRows;
    }

}