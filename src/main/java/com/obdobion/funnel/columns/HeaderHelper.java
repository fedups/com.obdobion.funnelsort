package com.obdobion.funnel.columns;

import java.util.Arrays;

import com.obdobion.algebrain.Equ;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

public class HeaderHelper extends ColumnHelper
{
    // final private static Logger logger =
    // LoggerFactory.getLogger(HeaderHelper.class);

    boolean waitingForInput = true;
    byte[]  originalHeaderRow;

    public HeaderHelper()
    {
        super();
    }

    public HeaderHelper(final int maxsize)
    {
        super(maxsize);
    }

    @Override
    public KeyContext extract (
        final FunnelContext funnelContext,
        final byte[] data,
        final long recordNumber,
        final int dataLength,
        final Equ... equations)
        throws Exception
    {
        originalHeaderRow = Arrays.copyOf(data, dataLength);
        /*
         * Add some more equations just for headerIn fields.
         */
        Equ[] cachedEquations;

        int ceSize = equations.length;
        ceSize += funnelContext.formatOutHelper.getReferencesToAllOutputFormatEquations(funnelContext).length;
        cachedEquations = new Equ[ceSize];

        int ce = 0;
        for (final Equ equ : equations)
            cachedEquations[ce++] = equ;
        for (final Equ equ : funnelContext.formatOutHelper.getReferencesToAllOutputFormatEquations(funnelContext))
            cachedEquations[ce++] = equ;

        return super.extract(funnelContext, data, recordNumber, dataLength, cachedEquations);
    }

    public byte[] getContents (final KeyPart headerCol) throws Exception
    {
        headerCol.parseObjectFromRawData(originalHeaderRow);
        return headerCol.getContentsAsByteArray();
    }

    public boolean isWaitingForInput ()
    {
        if (waitingForInput)
        {
            waitingForInput = false;
            if (columns != null && columns.size() > 0)
                return true;
        }
        return false;
    }
}
