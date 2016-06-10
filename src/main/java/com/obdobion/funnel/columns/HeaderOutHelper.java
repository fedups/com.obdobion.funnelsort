package com.obdobion.funnel.columns;

import java.io.IOException;

import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

public class HeaderOutHelper extends OutputFormatHelper
{
    boolean waitingToWrite = false;

    public HeaderOutHelper(final HeaderHelper _headerHelper)
    {
        super(null, _headerHelper);
    }

    public void format (final FunnelContext funnelContext, final ColumnWriter writer) throws IOException
    {
        if (formatter == null)
        {
            final int lengthToWrite = lengthToWrite(headerHelper.originalHeaderRow, 0, headerHelper.originalHeaderRow.length, false);
            writer.write(headerHelper.originalHeaderRow, 0, lengthToWrite);
            return;
        }
        try
        {
            final SourceProxyRecord dummyProxy = SourceProxyRecord.getInstance(funnelContext);
            dummyProxy.setOriginalRecordNumber(0);
            byte[] data = headerHelper.originalHeaderRow;
            if (data == null)
                data = new byte[0];
            dummyProxy.originalSize = data.length;
            extract(funnelContext, data, dummyProxy);
        } catch (final Exception e)
        {
            throw new IOException(e.getMessage(), e);
        }

        final int lengthToWrite = lengthToWrite(context.key, 0, context.keyLength, false);
        writer.write(context.key, 0, lengthToWrite);
    }

    public boolean isWaitingToWrite ()
    {
        if (waitingToWrite)
        {
            waitingToWrite = false;
            return true;
        }
        return false;
    }

    public void setWaitingToWrite (final boolean needsToBeWritten)
    {
        waitingToWrite = needsToBeWritten;
    }
}
