package com.obdobion.funnel.provider;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class FixedLengthSysinReader extends FixedLengthCacheReader
{
    static final private Logger _logger = Logger.getLogger(FixedLengthSysinReader.class);

    public FixedLengthSysinReader(final FunnelContext _context) throws IOException
    {
        super(_context);
    }

    @Override
    void loadDataToCache () throws IOException
    {
        context.inputCache = new InputCache(context, System.in);
        System.in.close();
        _logger.debug("loaded SYSIN");
    }

}
