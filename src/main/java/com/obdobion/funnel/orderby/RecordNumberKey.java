package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 * 
 */
public class RecordNumberKey extends KeyPart
{
    /**
     * @param dir
     * @param _parseFormat
     */
    public RecordNumberKey(final KeyDirection dir, final String _parseFormat)
    {
        super();
        offset = 0;
        length = 8;
        direction = dir;

        // assert parseFormat == null : "\"--format " + parseFormat +
        // "\"  is not expected for \"RecordNumber\"";
    }

    @Override
    public void format (final KeyContext context) throws Exception
    {
        Long longValue = (Long) parseObjectFromRawData(context);
        formatObjectIntoKey(context, longValue);

        if (nextPart != null)
            nextPart.format(context);
    }

    private void formatObjectIntoKey (final KeyContext context, Long _longValue)
    {
        Long longValue = _longValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            longValue = 0 - longValue;

        final ByteBuffer bb = ByteBuffer.wrap(context.key, context.keyLength, 8);
        bb.putLong(longValue ^ 0x8000000000000000L);
        context.keyLength += 8;
    }

    @Override
    public Object parseObjectFromRawData (KeyContext context)
    {
        return context.recordNumber;
    }
}
