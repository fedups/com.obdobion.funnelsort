package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>FixedLengthSysinReader class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FixedLengthSysinReader extends FixedLengthCacheReader
{
    static final private Logger _logger = LoggerFactory.getLogger(FixedLengthSysinReader.class);

    /**
     * <p>Constructor for FixedLengthSysinReader.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public FixedLengthSysinReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        _logger.debug("fixed length sysin provider activated");
    }

    @Override
    void loadDataToCache () throws IOException
    {
        context.inputCache = new FixedLengthInputCache(context, System.in);
        System.in.close();
        _logger.debug("loaded SYSIN");
    }

}
