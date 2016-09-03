package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * <p>
 * DisplayIntKey class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class DisplayIntKey extends KeyPart
{
    Long   contents;
    byte[] trimmed;

    /**
     * <p>
     * Constructor for DisplayIntKey.
     * </p>
     */
    public DisplayIntKey()
    {
        super();
    }

    /** {@inheritDoc} */
    @Override
    public Object getContents()
    {
        return contents;
    }

    /** {@inheritDoc} */
    @Override
    public double getContentsAsDouble()
    {
        return contents;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInteger()
    {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNumeric()
    {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void pack(final KeyContext context) throws Exception
    {
        parseObject(context);
        packObjectIntoKey(context, contents);

        if (nextPart != null)
            nextPart.pack(context);
    }

    /**
     * <p>
     * packObjectIntoKey.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.orderby.KeyContext} object.
     * @param _longValue a {@link java.lang.Long} object.
     */
    public void packObjectIntoKey(final KeyContext context, final Long _longValue)
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

    /** {@inheritDoc} */
    @SuppressWarnings("null")
    @Override
    public void parseObjectFromRawData(final byte[] rawBytes) throws Exception
    {
        if (trimmed == null)
            trimmed = new byte[length];

        int lengthThisTime = length;
        if (rawBytes == null)
            lengthThisTime = 0;
        else if (rawBytes.length < offset + length)
            lengthThisTime = rawBytes.length - offset;

        unformattedContents = Arrays.copyOfRange(rawBytes, offset, offset + lengthThisTime);

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
                continue;
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
        {
            contents = new Long(0);
            return;
        }

        contents = Long.parseLong(new String(trimmed, 0, t));
        if (minusSignFound)
            contents = -contents;
    }
}
