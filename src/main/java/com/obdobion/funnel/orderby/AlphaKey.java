package com.obdobion.funnel.orderby;

import com.obdobion.funnel.columns.OutputFormatHelper;

/**
 * @author Chris DeGreef
 *
 */
public class AlphaKey extends KeyPart
{
    static final private byte LowerA       = (byte) 'a';
    static final private byte LowerZ       = (byte) 'z';
    static final private byte LowerToUpper = (byte) ((byte) 'A' - LowerA);

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

    @Override
    public void pack (final KeyContext context) throws Exception
    {
        final byte[] rawBytes = ((String) parseObjectFromRawData(context)).getBytes();

        formatObjectIntoKey(context, rawBytes);

        if (nextPart != null)
            nextPart.pack(context);
    }

    @Override
    public Object parseObjectFromRawData (final KeyContext context) throws Exception
    {
        final byte[] bytes = rawBytes(context);

        int endOffset = this.offset;
        for (; endOffset < this.offset + this.length; endOffset++)
            if (bytes.length <= endOffset || bytes[endOffset] == 0)
                break;

        final int rightTrimmedLength = OutputFormatHelper
                .lengthToWrite(bytes, this.offset, endOffset - this.offset, true);
        return new String(bytes, this.offset, rightTrimmedLength);
    }
}