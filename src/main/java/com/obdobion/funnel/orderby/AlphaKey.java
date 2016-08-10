package com.obdobion.funnel.orderby;

import com.obdobion.funnel.columns.OutputFormatHelper;

/**
 * <p>AlphaKey class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AlphaKey extends KeyPart
{
    static final private byte LowerA       = (byte) 'a';
    static final private byte LowerZ       = (byte) 'z';
    static final private byte LowerToUpper = (byte) ((byte) 'A' - LowerA);

    String                    contents;

    /**
     * <p>Constructor for AlphaKey.</p>
     */
    public AlphaKey()
    {
        super();
        // assert parseFormat == null : "\"--format " + parseFormat +
        // "\"  is not expected for \"String\"";
    }

    private void formatObjectIntoKey (final KeyContext context, final byte[] rawBytes)
    {
        int lengthThisTime = length;
        if (rawBytes.length < length)
            lengthThisTime = rawBytes.length;

        System.arraycopy(rawBytes, 0, context.key, context.keyLength, lengthThisTime);
        context.keyLength += lengthThisTime;

        if (direction == KeyDirection.AASC || direction == KeyDirection.ADESC)
            for (int b = context.keyLength - lengthThisTime; b < context.keyLength; b++)
            {
                if (context.key[b] >= LowerA && context.key[b] <= LowerZ)
                    context.key[b] = (byte) (context.key[b] + LowerToUpper);
            }

        /*
         * Delimiters at the end of strings are necessary so that unequal length
         * comparisons stop comparing rather than continue to compare into the
         * next part of the key.
         * 
         * If the direction is DESC it is necessary to put an extra high-value
         * byte at the end of the keys so that the comparison of the generated
         * keys sorts shorter records to the end.
         */
        if (direction == KeyDirection.DESC || direction == KeyDirection.ADESC)
        {
            for (int b = context.keyLength - lengthThisTime; b < context.keyLength; b++)
            {
                context.key[b] = (byte) (context.key[b] ^ 0xff);
            }
            context.key[context.keyLength] = (byte) 0xff;
            context.keyLength++;
        } else
        {
            /*
             * It is necessary for ASC to put an extra low value byte at the end
             * so that shorter strings are sorted to the beginning.
             */
            context.key[context.keyLength] = (byte) 0x00;
            context.keyLength++;
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
        return contents.getBytes();
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

        formatObjectIntoKey(context, contents.getBytes());

        if (nextPart != null)
            nextPart.pack(context);
    }

    /** {@inheritDoc} */
    @Override
    public void parseObjectFromRawData (final byte[] bytes) throws Exception
    {
        int endOffset = this.offset;
        for (; endOffset < this.offset + this.length; endOffset++)
            if (bytes.length <= endOffset || bytes[endOffset] == 0)
                break;

        final int rightTrimmedLength = OutputFormatHelper
                .lengthToWrite(bytes, this.offset, endOffset - this.offset, true);
        contents = new String(bytes, this.offset, rightTrimmedLength);
        unformattedContents = contents.getBytes();
    }
}
