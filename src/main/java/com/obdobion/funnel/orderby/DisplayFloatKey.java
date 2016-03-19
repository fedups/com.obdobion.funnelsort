package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 * 
 */
public class DisplayFloatKey extends KeyPart
{
    byte[] trimmed;

    public DisplayFloatKey()
    {
        super();
    }

    @Override
    public void format (final KeyContext context) throws Exception
    {
        Double doubleValue = (Double) parseObjectFromRawData(context);
        formatObjectIntoKey(context, doubleValue);

        if (nextPart != null)
            nextPart.format(context);
    }

    private void formatObjectIntoKey (final KeyContext context, Double _doubleValue)
    {
        Double doubleValue = _doubleValue;

        if (doubleValue < 0)
            if (direction == KeyDirection.AASC || direction == KeyDirection.ADESC)
                doubleValue = 0 - doubleValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            doubleValue = 0 - doubleValue;

        final ByteBuffer bb = ByteBuffer.wrap(context.key, context.keyLength, 8);

        long longbits = Double.doubleToRawLongBits(doubleValue);
        if (doubleValue < 0)
        {
            longbits = longbits ^ 0xffffffffffffffffL;
        } else
        {
            longbits = longbits ^ 0x8000000000000000L;
        }

        bb.putLong(longbits);
        context.keyLength += 8;
    }

    @Override
    public Object parseObjectFromRawData (KeyContext context)
    {
        if (trimmed == null)
            trimmed = new byte[length];

        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (rawBytes.length < offset + length)
            lengthThisTime = rawBytes.length - offset;

        int t = 0;
        boolean minusSignFound = false;
        for (int b = 0; b < lengthThisTime; b++)
        {
            final byte bb = rawBytes[offset + b];
            if (bb >= (byte) '0')
                if (bb <= (byte) '9')
                {
                    trimmed[t++] = bb;
                    continue;
                }
            if (bb == (byte) ',')
            {
                continue;
            }
            if (bb == (byte) '-')
            {
                minusSignFound = true;
                continue;
            }
            if (bb == (byte) '.')
            {
                trimmed[t++] = bb;
                continue;
            }
            if (t > 0)
                break;
        }

        double doubleValue = Double.parseDouble(new String(trimmed, 0, t));
        if (minusSignFound)
            doubleValue = -doubleValue;
        return doubleValue;
    }
}
