package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chris DeGreef
 *
 */
public class DateKey extends KeyPart
{
    static final private Logger logger           = LoggerFactory.getLogger(DateKey.class);

    SimpleDateFormat            sdf;
    public long                 dateFormatErrors = 0;

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
    public void pack (final KeyContext context) throws Exception
    {
        final Calendar cal = (Calendar) parseObjectFromRawData(context);
        formatObjectIntoKey(context, cal.getTimeInMillis());

        if (nextPart != null)
            nextPart.pack(context);
    }

    @Override
    public Object parseObjectFromRawData (final KeyContext context)
    {
        final Calendar calendar = Calendar.getInstance();

        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (rawBytes.length < offset + length)
            lengthThisTime = rawBytes.length - offset;

        final String trimmed = new String(rawBytes, offset, lengthThisTime).trim();
        long longValue = 0;

        if (trimmed.length() > 0)
        {
            try
            {
                if (sdf == null)
                    sdf = new SimpleDateFormat(parseFormat);
                final Date date = sdf.parse(trimmed);
                longValue = date.getTime();
            } catch (final Exception e)
            {
                if (dateFormatErrors == 0)
                    logger.warn("date format errors: " + trimmed);
                dateFormatErrors++;
            }
        }
        calendar.setTimeInMillis(longValue);
        return calendar;
    }
}
