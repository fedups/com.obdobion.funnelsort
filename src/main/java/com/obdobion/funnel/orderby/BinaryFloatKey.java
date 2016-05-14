package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 *
 */
public class BinaryFloatKey extends KeyPart
{
    public BinaryFloatKey()
    {
        super();
        // assert parseFormat == null : "\"--format " + parseFormat +
        // "\"  is not expected for \"BFloat\"";
        // assert length == 4 || length == 8 :
        // "Binary Float lengths must be 4 or 8.";
    }

    @SuppressWarnings("incomplete-switch")
    private void formatObjectIntoKey (final KeyContext _context, final Double _doubleValue)
    {
        Double doubleValue = _doubleValue;

        if (doubleValue < 0)
            if (direction == KeyDirection.AASC || direction == KeyDirection.ADESC)
                doubleValue = 0 - doubleValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            doubleValue = 0 - doubleValue;

        final ByteBuffer bb = ByteBuffer.wrap(_context.key, _context.keyLength, 8);

        switch (length)
        {
            case 4:
                int intbits = (int) Double.doubleToRawLongBits(doubleValue);
                if (doubleValue < 0)
                {
                    intbits = intbits ^ 0xffffffff;
                } else
                {
                    intbits = intbits ^ 0x80000000;
                }

                bb.putLong(intbits);
                break;
            case 8:
                long longbits = Double.doubleToRawLongBits(doubleValue);
                if (doubleValue < 0)
                {
                    longbits = longbits ^ 0xffffffffffffffffL;
                } else
                {
                    longbits = longbits ^ 0x8000000000000000L;
                }

                bb.putLong(longbits);
                break;
        }
        _context.keyLength += length;
    }

    @Override
    public void pack (final KeyContext _context) throws Exception
    {
        final Double _doubleValue = (Double) parseObjectFromRawData(_context);
        formatObjectIntoKey(_context, _doubleValue);

        if (nextPart != null)
            nextPart.pack(_context);
    }

    @SuppressWarnings("incomplete-switch")
    @Override
    public Object parseObjectFromRawData (final KeyContext context) throws Exception
    {
        final byte[] rawBytes = rawBytes(context);

        if (rawBytes.length < offset + length)
            throw new Exception("index out of bounds: " + (offset + length));

        final ByteBuffer bb = ByteBuffer.wrap(rawBytes, offset, 8);

        switch (length)
        {
            case 4:
                return new Double(bb.getFloat());
            case 8:
                return bb.getDouble();
        }
        throw new Exception("invalid length for a binary floating point field: " + length);
    }
}
