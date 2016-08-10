package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

/**
 * <p>DateKey class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class DateKey extends KeyPart
{
    Calendar         contents;
    SimpleDateFormat sdf;

    /**
     * <p>Constructor for DateKey.</p>
     */
    public DateKey()
    {
        super();
    }

    private void formatObjectIntoKey (final KeyContext context, final Long _longValue)
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
    @Override
    public Object getContents ()
    {
        return contents;
    }

    /** {@inheritDoc} */
    @Override
    public double getContentsAsDouble ()
    {
        return contents.getTimeInMillis();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isDate ()
    {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNumeric ()
    {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void pack (final KeyContext context) throws Exception
    {
        parseObject(context);
        formatObjectIntoKey(context, contents.getTimeInMillis());

        if (nextPart != null)
            nextPart.pack(context);
    }

    /** {@inheritDoc} */
    @Override
    public void parseObjectFromRawData (final byte[] rawBytes) throws Exception
    {
        contents = Calendar.getInstance();

        int lengthThisTime = length;
        if (rawBytes.length < offset + length)
            lengthThisTime = rawBytes.length - offset;

        final String trimmed = new String(rawBytes, offset, lengthThisTime).trim();
        unformattedContents = Arrays.copyOfRange(rawBytes, offset, offset + lengthThisTime);

        long longValue = 0;

        if (trimmed.length() > 0)
        {
            if (sdf == null)
                sdf = new SimpleDateFormat(parseFormat);
            final Date date = sdf.parse(trimmed);
            longValue = date.getTime();
        }
        contents.setTimeInMillis(longValue);
    }
}
