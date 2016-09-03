package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * VariableLengthSysinReader class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class VariableLengthSysinReader extends VariableLengthCacheReader
{
    static final private Logger _logger = LoggerFactory.getLogger(VariableLengthSysinReader.class);

    /**
     * <p>
     * Constructor for VariableLengthSysinReader.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public VariableLengthSysinReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        logger.debug("variable length sysin reader activated");
    }

    @Override
    void loadDataToCache() throws IOException
    {
        context.inputCache = new VariableLengthInputCache(context, System.in);
        System.in.close();
        _logger.debug("loaded SYSIN");
    }
}
