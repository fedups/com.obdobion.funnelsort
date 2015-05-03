package com.obdobion.funnel.provider;

import java.io.IOException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class ProviderFactory
{
    static public FunnelDataProvider create (FunnelContext context) throws IOException
    {
        if (context.fixedRecordLength > 0)
            return new FixedLengthProvider(context);
        if (context.csv == null)
            return new VariableLengthProvider(context);
        return new VariableLengthCsvProvider(context);
    }
}
