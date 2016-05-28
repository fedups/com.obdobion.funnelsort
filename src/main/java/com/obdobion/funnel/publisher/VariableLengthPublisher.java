package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
abstract public class VariableLengthPublisher extends AbstractPublisher
{
    public VariableLengthPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        this.originalBytes = new byte[1024];
    }

    @Override
    void formatOutputAndWrite (final SourceProxyRecord item, final byte[] rawData)
        throws IOException, Exception
    {
        context.formatOutHelper.format(this, rawData, item.originalSize, item, true);
        write(context.endOfRecordDelimiterOut, 0, context.endOfRecordDelimiterOut.length);
        super.formatOutputAndWrite(item, rawData);
    }

    @Override
    void loadOriginalBytes (final int originalFileNumber, final SourceProxyRecord item)
        throws IOException
    {
        if (item.originalSize > originalBytes.length)
        {
            originalBytes = new byte[item.originalSize + 1024];
        }
        /*
         * Make sure to delimit the current record length in the input buffer.
         */
        originalBytes[item.originalSize] = 0x00;
        super.loadOriginalBytes(originalFileNumber, item);
    }

    @Override
    void publishHeader () throws IOException
    {
        /*
         * This is the first time publishing to this file. So lets see if there
         * is a header tucked away in the csv context area. We will write that
         * out first.
         */
        if (context.csv != null && context.csv.header && context.csv.headerContents != null)
        {
            write(context.csv.headerContents, 0, context.csv.headerContents.length);
            write(context.endOfRecordDelimiterOut, 0, context.endOfRecordDelimiterOut.length);
        }
    }
}