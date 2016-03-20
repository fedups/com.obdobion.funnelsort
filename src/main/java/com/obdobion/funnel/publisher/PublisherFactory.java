package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class PublisherFactory
{
    static public FunnelDataPublisher create (final FunnelContext context) throws ParseException, IOException
    {
        if (context.fixedRecordLength == 0 || context.variableLengthOutput)
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
