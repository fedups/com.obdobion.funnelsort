package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthSysinReader extends VariableLengthCacheReader
{
    static final private Logger _logger = Logger.getLogger(VariableLengthSysinReader.class);

    public VariableLengthSysinReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        logger.debug("variable length sysin provider activated");
    }

    @Override
    void loadDataToCache () throws IOException
    {
        context.inputCache = new InputCache(context, System.in);
        System.in.close();
        _logger.debug("loaded SYSIN");
    }
}
