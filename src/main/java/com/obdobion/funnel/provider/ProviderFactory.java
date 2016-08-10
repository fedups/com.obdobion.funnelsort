package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>ProviderFactory class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class ProviderFactory
{
    /**
     * <p>create.</p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @return a {@link com.obdobion.funnel.FunnelDataProvider} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    static public FunnelDataProvider create (final FunnelContext context) throws IOException, ParseException
    {
        if (context.getFixedRecordLengthIn() > 0)
            return new FixedLengthProvider(context);
        if (context.getCsv() == null)
            return new VariableLengthProvider(context);
        return new CsvProvider(context);
    }
}
