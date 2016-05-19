package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.InputStream;

import com.obdobion.funnel.parameters.FunnelContext;

public class VariableLengthInputCache extends AbstractInputCache
{
    public VariableLengthInputCache(final FunnelContext _context, final InputStream _source) throws IOException
    {
        super(_context, _source);
    }

    @Override
    void postOpenVerification () throws IOException
    {
        // nothing to do here
    }
}
