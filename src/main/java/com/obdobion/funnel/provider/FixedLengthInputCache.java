package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.InputStream;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>FixedLengthInputCache class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FixedLengthInputCache extends AbstractInputCache
{
    /**
     * <p>Constructor for FixedLengthInputCache.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @param _source a {@link java.io.InputStream} object.
     * @throws java.io.IOException if any.
     */
    public FixedLengthInputCache(final FunnelContext _context, final InputStream _source) throws IOException
    {
        super(_context, _source);
    }

    @Override
    void postOpenVerification () throws IOException
    {
        if (length() % context.getFixedRecordLengthIn() != 0)
            throw new IOException("file size ("
                + length()
                + ") not even multiple of record size ("
                + context.getFixedRecordLengthIn()
                + ")");
    }
}
