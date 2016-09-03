package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.InputStream;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * VariableLengthInputCache class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class VariableLengthInputCache extends AbstractInputCache
{
    /**
     * <p>
     * Constructor for VariableLengthInputCache.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @param _source a {@link java.io.InputStream} object.
     * @throws java.io.IOException if any.
     */
    public VariableLengthInputCache(final FunnelContext _context, final InputStream _source) throws IOException
    {
        super(_context, _source);
    }

    @Override
    void postOpenVerification() throws IOException
    {
        // nothing to do here
    }
}
