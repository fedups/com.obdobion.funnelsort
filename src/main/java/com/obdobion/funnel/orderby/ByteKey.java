package com.obdobion.funnel.orderby;

import java.util.Arrays;

/**
 * @author Chris DeGreef
 *
 */
public class ByteKey extends KeyPart
{
    byte[] contents;

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

    @Override
    public Object getContents ()
    {
        return contents;
    }

    @Override
    public byte[] getContentsAsByteArray ()
    {
        return contents;
    }

    @Override
    public double getContentsAsDouble ()
    {
        return 0;
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

        formatObjectIntoKey(context, contents);

        if (nextPart != null)
            nextPart.pack(context);
    }

    @Override
    public void parseObjectFromRawData (final KeyContext context) throws Exception
    {
        contents = Arrays.copyOfRange(rawBytes(context), this.offset, this.offset + this.length);
    }
}