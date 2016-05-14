package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 *
 */
public class DisplayIntKey extends KeyPart
{
    byte[] trimmed;

    public DisplayIntKey()
    {
        super();
    }

    @Override
    public void pack (final KeyContext context) throws Exception
    {
        final Long longValue = (Long) parseObjectFromRawData(context);
        packObjectIntoKey(context, longValue);

        if (nextPart != null)
            nextPart.pack(context);
    }

    public void packObjectIntoKey (final KeyContext context, final Long _longValue)
    {
        Long longValue = _longValue;

        if (longValue < 0)
            if (direction == KeyDirection.AASC || direction == KeyDirection.ADESC)
                longValue = 0 - longValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            longValue = 0 - longValue;

        final ByteBuffer bb = ByteBuffer.wrap(context.key, context.keyLength, 8);
        /*
         * Flip the sign bit so negatives are before positives in ascending
         * sorts.
         */
        bb.putLong(longValue ^ 0x8000000000000000L);
        context.keyLength += 8;
    }

    @SuppressWarnings("null")
    @Override
    public Object parseObjectFromRawData (final KeyContext context)
    {
        if (trimmed == null)
            trimmed = new byte[length];

        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (rawBytes == null)
            lengthThisTime = 0;
        else if (rawBytes.length < offset + length)
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
                break;
            if (t > 0)
                break;
        }

        if (trimmed[0] == 0x00)
            return new Long(0);

        Long longValue = Long.parseLong(new String(trimmed, 0, t));
        if (minusSignFound)
            longValue = -longValue;
        return longValue;
    }
}
