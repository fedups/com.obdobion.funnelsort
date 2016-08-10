package com.obdobion.funnel.orderby;

import java.util.Arrays;

/**
 * <p>ByteKey class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class ByteKey extends KeyPart
{
    byte[] contents;

    /**
     * <p>Constructor for ByteKey.</p>
     */
    public ByteKey()
    {
        super();
    }

    private void formatObjectIntoKey (final KeyContext context, final byte[] rawBytes)
    {
        int lengthThisTime = length;
        if (rawBytes.length < length)
            lengthThisTime = rawBytes.length;

        System.arraycopy(rawBytes, 0, context.key, context.keyLength, lengthThisTime);
        context.keyLength += lengthThisTime;

        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
        {
            for (int b = context.keyLength - lengthThisTime; b < context.keyLength; b++)
            {
                context.key[b] = (byte) (context.key[b] ^ 0xff);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object getContents ()
    {
        return contents;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] getContentsAsByteArray ()
    {
        return contents;
    }

    /** {@inheritDoc} */
    @Override
    public double getContentsAsDouble ()
    {
        return 0;
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

        formatObjectIntoKey(context, contents);

        if (nextPart != null)
            nextPart.pack(context);
    }

    /** {@inheritDoc} */
    @Override
    public void parseObjectFromRawData (final byte[] rawBytes) throws Exception
    {
        contents = Arrays.copyOfRange(rawBytes, this.offset, this.offset + this.length);
        unformattedContents = contents;
    }
}
