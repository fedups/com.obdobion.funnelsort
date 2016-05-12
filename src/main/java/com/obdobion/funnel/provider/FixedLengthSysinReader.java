package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthSysinReader extends FixedLengthCacheReader
{
    static final private Logger _logger = Logger.getLogger(FixedLengthSysinReader.class);

    public FixedLengthSysinReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        logger.info("fixed length sysin provider activated");
    }

    @Override
    void loadDataToCache () throws IOException
    {
        context.inputCache = new InputCache(context, System.in);
        System.in.close();
        _logger.debug("loaded SYSIN");
    }

}
