package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * @author Chris DeGreef
 *
 */
public class BinaryIntKey extends KeyPart
{
    public BinaryIntKey()
    {
        super();
        // assert parseFormat == null : "\"--format " + parseFormat +
        // "\"  is not expected for \"BInteger\"";
        // assert length == 1 || length == 2 || length == 4 || length == 8 :
        // "Binary Integer lengths must be 1, 2, 4, or 8.";
    }

    @SuppressWarnings("incomplete-switch")
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
        switch (length)
        {
            case 1:
                bb.put((byte) ((longValue.byteValue()) ^ 0x80));
                break;
            case 2:
                bb.putShort((short) ((longValue.shortValue()) ^ 0x8000));
                break;
            case 4:
                bb.putInt(((longValue.intValue()) ^ 0x80000000));
                break;
            case 8:
                bb.putLong(longValue ^ 0x8000000000000000L);
                break;
        }
        context.keyLength += length;
    }

    @Override
    public void pack (final KeyContext context) throws Exception
    {
        final Long _longValue = (Long) parseObjectFromRawData(context);
        formatObjectIntoKey(context, _longValue);

        if (nextPart != null)
            nextPart.pack(context);
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
            case 1:
                return new Long(bb.get());
            case 2:
                return new Long(bb.getShort());
            case 4:
                return new Long(bb.getInt());
            case 8:
                return new Long(bb.getLong());
        }
        throw new Exception("invalid length for a binary integer field: " + length);
    }
}
