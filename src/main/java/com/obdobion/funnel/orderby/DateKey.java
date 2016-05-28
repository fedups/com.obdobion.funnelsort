package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author Chris DeGreef
 *
 */
public class DateKey extends KeyPart
{
    Calendar         contents;
    SimpleDateFormat sdf;

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

    @Override
    public Object getContents ()
    {
        return contents;
    }

    @Override
    public double getContentsAsDouble ()
    {
        return contents.getTimeInMillis();
    }

    @Override
    public boolean isDate ()
    {
        return true;
    }

    @Override
    public boolean isNumeric ()
    {
        return false;
    }

    @Override
    public void pack (final KeyContext context) throws Exception
    {
        parseObjectFromRawData(context);
        formatObjectIntoKey(context, contents.getTimeInMillis());

        if (nextPart != null)
            nextPart.pack(context);
    }

    @Override
    public void parseObjectFromRawData (final KeyContext context) throws ParseException
    {
        contents = Calendar.getInstance();

        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (rawBytes.length < offset + length)
            lengthThisTime = rawBytes.length - offset;

        final String trimmed = new String(rawBytes, offset, lengthThisTime).trim();
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
