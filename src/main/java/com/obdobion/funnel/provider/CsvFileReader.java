package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * CsvFileReader class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class CsvFileReader extends VariableLengthFileReader
{

    static final private Logger _logger = LoggerFactory.getLogger(CsvFileReader.class);

    /**
     * <p>
     * Constructor for CsvFileReader.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public CsvFileReader(final FunnelContext _context) throws IOException, ParseException
    {
        this(_context, defaultCharBufferSize);
        _logger.debug("csv file reader activated");
    }

    /**
     * <p>
     * Constructor for CsvFileReader.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @param sz a int.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public CsvFileReader(final FunnelContext _context, final int sz) throws IOException, ParseException
    {
        super(_context, sz);
    }

}
