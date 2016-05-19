package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.InputStream;

import com.obdobion.funnel.parameters.FunnelContext;

public class FixedLengthInputCache extends AbstractInputCache
{
    public FixedLengthInputCache(final FunnelContext _context, final InputStream _source) throws IOException
    {
        super(_context, _source);
    }

    @Override
    void postOpenVerification () throws IOException
    {
        if (length() % context.fixedRecordLengthIn != 0)
            throw new IOException("file size ("
                + length()
                + ") not even multiple of record size ("
                + context.fixedRecordLengthIn
                + ")");
    }
}
