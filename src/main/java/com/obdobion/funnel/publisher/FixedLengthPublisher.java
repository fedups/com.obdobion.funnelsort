package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
abstract public class FixedLengthPublisher extends AbstractPublisher
{
    public FixedLengthPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        this.originalBytes = new byte[Math.max(_context.fixedRecordLengthOut, _context.fixedRecordLengthIn)];
    }

    @Override
    void formatOutputAndWrite (final SourceProxyRecord item, final byte[] rawData)
        throws IOException, Exception
    {
        context.formatOutHelper.format(this, originalBytes, context.fixedRecordLengthOut, item, false);
        super.formatOutputAndWrite(item, rawData);
    }

    @Override
    void loadOriginalBytes (final int originalFileNumber, final SourceProxyRecord item)
        throws IOException
    {
        /*
         * Make sure to delimit the current record length in the input buffer.
         */
        for (int b = 0; b < originalBytes.length; b++)
            originalBytes[b] = ' ';
        super.loadOriginalBytes(originalFileNumber, item);
    }
}