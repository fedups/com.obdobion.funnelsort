package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * PublisherFactory class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class PublisherFactory
{
    /**
     * <p>
     * create.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @return a {@link com.obdobion.funnel.FunnelDataPublisher} object.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    static public FunnelDataPublisher create(final FunnelContext context) throws ParseException, IOException
    {
        if (context.getFixedRecordLengthOut() == 0)
        {
            if (context.isSysout())
                return new VariableLengthSysoutPublisher(context);
            return new VariableLengthFilePublisher(context);
        }
        if (context.isSysout())
            return new FixedLengthSysoutPublisher(context);
        return new FixedLengthFilePublisher(context);
    }
}
