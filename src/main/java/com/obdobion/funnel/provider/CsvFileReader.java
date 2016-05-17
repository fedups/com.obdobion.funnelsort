package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class CsvFileReader extends VariableLengthFileReader
{

    static final private Logger _logger = LoggerFactory.getLogger(CsvFileReader.class);

    public CsvFileReader(final FunnelContext _context) throws IOException, ParseException
    {
        this(_context, defaultCharBufferSize);
        _logger.debug("csv file reader activated");
    }

    public CsvFileReader(final FunnelContext _context, final int sz) throws IOException, ParseException
    {
        super(_context, sz);
    }

}
