package com.obdobion.funnel.columns;

import java.util.Arrays;

import com.obdobion.algebrain.Equ;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * HeaderHelper class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class HeaderHelper extends ColumnHelper
{
    // final private static Logger logger =
    // LoggerFactory.getLogger(HeaderHelper.class);

    boolean waitingForInput = true;
    byte[]  originalHeaderRow;

    /**
     * <p>
     * Constructor for HeaderHelper.
     * </p>
     */
    public HeaderHelper()
    {
        super();
    }

    /**
     * <p>
     * Constructor for HeaderHelper.
     * </p>
     *
     * @param maxsize a int.
     */
    public HeaderHelper(final int maxsize)
    {
        super(maxsize);
    }

    /** {@inheritDoc} */
    @Override
    public KeyContext extract(
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

    /**
     * <p>
     * getContents.
     * </p>
     *
     * @param headerCol a {@link com.obdobion.funnel.orderby.KeyPart} object.
     * @return an array of byte.
     * @throws java.lang.Exception if any.
     */
    public byte[] getContents(final KeyPart headerCol) throws Exception
    {
        headerCol.parseObjectFromRawData(originalHeaderRow);
        return headerCol.getContentsAsByteArray();
    }

    /**
     * <p>
     * isWaitingForInput.
     * </p>
     *
     * @return a boolean.
     */
    public boolean isWaitingForInput()
    {
        if (waitingForInput)
        {
            waitingForInput = false;
            // if (columns != null && columns.size() > 0)
            return true;
        }
        return false;
    }

    /**
     * <p>
     * Setter for the field <code>waitingForInput</code>.
     * </p>
     *
     * @param shouldReadHeader a boolean.
     */
    public void setWaitingForInput(final boolean shouldReadHeader)
    {
        waitingForInput = shouldReadHeader;
    }
}
