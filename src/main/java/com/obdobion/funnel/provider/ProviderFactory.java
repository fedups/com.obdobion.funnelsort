package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class ProviderFactory
{
    static public FunnelDataProvider create (final FunnelContext context) throws IOException, ParseException
    {
        if (context.fixedRecordLengthIn > 0)
            return new FixedLengthProvider(context);
        if (context.csv == null)
            return new VariableLengthProvider(context);
        return new VariableLengthCsvProvider(context);
    }
}
