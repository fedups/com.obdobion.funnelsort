package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 *
 */
public class RecordNumberKey extends KeyPart
{
    Long contents;

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

    private void formatObjectIntoKey (final KeyContext context, final Long _longValue)
    {
        Long longValue = _longValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            longValue = 0 - longValue;

        final ByteBuffer bb = ByteBuffer.wrap(context.key, context.keyLength, 8);
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
        return contents;
    }

    @Override
    public boolean isInteger ()
    {
        return true;
    }

    @Override
    public boolean isNumeric ()
    {
        return true;
    }

    @Override
    public void pack (final KeyContext context) throws Exception
    {
        parseObject(context);
        formatObjectIntoKey(context, contents);

        if (nextPart != null)
            nextPart.pack(context);
    }

    @Override
    public void parseObject (final KeyContext context)
    {
        contents = context.recordNumber;
    }

    @Override
    public void parseObjectFromRawData (final byte[] rawBytes) throws Exception
    {
        // not used since this is a system variable
    }
}
