package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * CsvCacheReader class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class CsvCacheReader extends VariableLengthCacheReader
{

    static final private Logger _logger = LoggerFactory.getLogger(CsvFileReader.class);

    /**
     * <p>
     * Constructor for CsvCacheReader.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public CsvCacheReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        _logger.debug("csv cache reader activated");
    }
}
