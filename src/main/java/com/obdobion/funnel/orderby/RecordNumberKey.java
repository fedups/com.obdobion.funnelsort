package com.obdobion.funnel.orderby;

import java.nio.ByteBuffer;

/**
 * <p>
 * RecordNumberKey class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class RecordNumberKey extends KeyPart
{
    Long contents;

    /**
     * <p>
     * Constructor for RecordNumberKey.
     * </p>
     *
     * @param dir a {@link com.obdobion.funnel.orderby.KeyDirection} object.
     * @param _parseFormat a {@link java.lang.String} object.
     */
    public RecordNumberKey(final KeyDirection dir, final String _parseFormat)
    {
        super();
        offset = 0;
        length = 8;
        direction = dir;

        // assert parseFormat == null : "\"--format " + parseFormat +
        // "\" is not expected for \"RecordNumber\"";
    }

    private void formatObjectIntoKey(final KeyContext context, final Long _longValue)
    {
        Long longValue = _longValue;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
            longValue = 0 - longValue;

        final ByteBuffer bb = ByteBuffer.wrap(context.key, context.keyLength, 8);
        bb.putLong(longValue ^ 0x8000000000000000L);
        context.keyLength += 8;
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
        formatObjectIntoKey(context, contents);

        if (nextPart != null)
            nextPart.pack(context);
    }

    /** {@inheritDoc} */
    @Override
    public void parseObject(final KeyContext context)
    {
        contents = context.recordNumber;
    }

    /** {@inheritDoc} */
    @Override
    public void parseObjectFromRawData(final byte[] rawBytes) throws Exception
    {
        // not used since this is a system variable
    }
}
