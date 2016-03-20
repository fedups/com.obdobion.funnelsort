package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthCsvProvider extends VariableLengthProvider
{

    static final Logger _logger = Logger.getLogger(VariableLengthCsvProvider.class);

    boolean             includeColumn[];

    /**
     * for testing only
     *
     * @param context
     * @throws IOException
     */
    public VariableLengthCsvProvider(final boolean _includeColumn[]) throws Exception
    {
        this(new FunnelContext());
        this.includeColumn = _includeColumn;
    }

    public VariableLengthCsvProvider(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        if (_context == null || _context.keys == null)
            return;
        /*
         * Find out which fields we really care about. No sense in moving around
         * bytes or analyzing fields we ultimately won't care about.
         */
        int highestKeyedColumnNumber = -1;
        for (final KeyPart kdef : _context.keys)
        {
            if (kdef.csvFieldNumber > highestKeyedColumnNumber)
                highestKeyedColumnNumber = kdef.csvFieldNumber;
        }
        includeColumn = new boolean[highestKeyedColumnNumber + 1];
        for (final KeyPart kdef : _context.keys)
            includeColumn[kdef.csvFieldNumber] = true;
    }

    @Override
    public long actualNumberOfRows ()
    {
        return super.actualNumberOfRows()
            - (context.csv.header
                    ? 1
                    : 0);
    }

    public byte[][] decodeCsv (final byte[] input, final int inputLength, final byte quoteByte, final byte separatorByte)
    {
        final byte[][] field = new byte[includeColumn.length][];

        boolean inQuotes = false;
        int fNum = 0;
        int currentStart = 0;
        int locationOfNextByteAfterField;
        for (locationOfNextByteAfterField = 0; locationOfNextByteAfterField < inputLength; locationOfNextByteAfterField++)
        {
            if (fNum >= includeColumn.length)
                return field;
            /*
             * The length does not help because it is the length of the whole
             * row and at this point we don't know the context of this specific
             * field within the scope of the whole row. So we have to settle for
             * looking for the hex0. And since this is a variable length file it
             * is not reasonable to have hex data within the row. And this
             * coding means that any specific field can only be accessed up to
             * the first 0 or its delimiter of it is not the last field.
             */
            if (input[locationOfNextByteAfterField] == 0)
                break;

            if (inQuotes)
            {
                if (input[locationOfNextByteAfterField] == quoteByte)
                {
                    inQuotes = false;
                }
                continue;
            }
            if (input[locationOfNextByteAfterField] == separatorByte)
            {
                if (fNum < includeColumn.length)
                    if (includeColumn[fNum])
                    {
                        field[fNum] = unquote(input, currentStart, locationOfNextByteAfterField - 1, quoteByte);
                    }
                fNum++;
                currentStart = locationOfNextByteAfterField + 1;
                continue;
            }
            if (input[locationOfNextByteAfterField] == quoteByte)
            {
                inQuotes = true;
                continue;
            }
        }

        if (fNum < includeColumn.length)
            if (includeColumn[fNum])
                field[fNum] = unquote(input, currentStart, locationOfNextByteAfterField - 1, quoteByte);

        return field;
    }

    /**
     * A special case of not selecting the row is when a header is expected on
     * the csv file. This is one line and is to be kept for writing but ignored
     * for sorting. The publisher needs to be sensitive to the csv header
     * possibility and force the header out at the front of the file.
     *
     */
    @Override
    boolean isRowSelected (final int byteCount)
    {
        if (context.csv.header && context.csv.headerContents == null)
        {
            context.csv.headerContents = new byte[byteCount];
            System.arraycopy(row, 0, context.csv.headerContents, 0, byteCount);
            return false;
        }
        return true;
    }

    @Override
    KeyContext postReadKeyProcessing (final int byteCount) throws IOException
    {
        KeyContext kContext = null;
        try
        {
            final byte[][] data = decodeCsv(row, byteCount, context.csv.quoteByte, context.csv.separatorByte);
            kContext = context.keyHelper.extractKey(data, recordNumber);

        } catch (final Exception e)
        {
            throw new IOException(e);
        }
        return kContext;
    }

    byte[] unquote (final byte[] input, final int _start, final int _end, final byte quoteByte)
    {
        int start = _start;
        int end = _end;

        for (; start <= end; start++)
        {
            if (input[start] == (byte) ' ' || input[start] == (byte) '\t' || input[start] == quoteByte)
                continue;
            break;
        }
        for (; start <= end; end--)
        {
            if (input[end] == 32 || input[end] == 8 || input[end] == quoteByte)
                continue;
            break;
        }
        final int len = (end - start) + 1;
        final byte[] bb = new byte[len];
        System.arraycopy(input, start, bb, 0, len);
        return bb;
    }
}