package com.obdobion.funnel.orderby;

import java.io.ByteArrayOutputStream;

import com.obdobion.argument.annotation.Arg;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * <p>Abstract KeyPart class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
abstract public class KeyPart
{
    public KeyPart      nextPart;
    byte[]              unformattedContents;

    /*
     * KeyPart is used as an internal representation of orderby. This most a
     * legacy issue. This direction is not used as an argument. Keytype is used
     * as an argument for several things like headers, columns and the like,
     * that have no direction associated with them. Bad design - but for now, it
     * is what it is.
     */
    public KeyDirection direction;

    @Arg(shortName = 'n', longName = "name", help = "The key name", caseSensitive = true)
    public String       columnName;

    @Arg(shortName = 't', longName = "type", positional = true, help = "The data type of the key", required = true)
    public KeyType      typeName;

    @Arg(shortName = 'f',
            longName = "field",
            range = { "1" },
            help = "If this is a CSV file then use this instead of offset and length.  The first field is field #1 (not zero).")
    public int          csvFieldNumber;

    @Arg(shortName = 'o',
            defaultValues = { "-1" },
            range = { "0" },
            help = "The zero relative offset from the beginning of a row.  This will be computed, if not specified, to be the location of the previous column plus the length of the previous column.  Most often this parameter is not needed.")
    public int          offset;

    @Arg(shortName = 'l',
            defaultValues = { "255" },
            range = { "1", "255" },
            help = "The length of the key in bytes.")
    public int          length;

    @Arg(shortName = 'd',
            longName = "format",
            caseSensitive = true,
            help = "The parsing format for converting the contents of the key in the file to an internal representation. Use Java SimpleDateFormat rules for making the format.")
    public String       parseFormat;

    /**
     * <p>Constructor for KeyPart.</p>
     */
    public KeyPart()
    {
        super();
        csvFieldNumber = -1;
        typeName = null;
        columnName = null;
        offset = -1;
    }

    /**
     * <p>add.</p>
     *
     * @param anotherFormatter a {@link com.obdobion.funnel.orderby.KeyPart} object.
     */
    public void add(final KeyPart anotherFormatter)
    {
        if (nextPart == null)
            nextPart = anotherFormatter;
        else
            nextPart.add(anotherFormatter);
    }

    /**
     * Copy everything exception the key specific things like direction and
     * nextPart. Even if this is the key that caused the column to be defined we
     * can still copy the values since they would be the same.
     *
     * @param colDef a {@link com.obdobion.funnel.orderby.KeyPart} object.
     */
    public void defineFrom(final KeyPart colDef)
    {
        csvFieldNumber = colDef.csvFieldNumber;
        offset = colDef.offset;
        length = colDef.length;
        parseFormat = colDef.parseFormat;
        typeName = colDef.typeName;
        columnName = colDef.columnName;
    }

    /**
     * <p>getContents.</p>
     *
     * @return a {@link java.lang.Object} object.
     */
    abstract public Object getContents();

    /**
     * <p>getContentsAsByteArray.</p>
     *
     * @return an array of byte.
     */
    public byte[] getContentsAsByteArray()
    {
        return unformattedContents;
    }

    /**
     * <p>getContentsAsDouble.</p>
     *
     * @return a double.
     */
    abstract public double getContentsAsDouble();

    /**
     * <p>isCsv.</p>
     *
     * @return a boolean.
     */
    public boolean isCsv()
    {
        return csvFieldNumber >= 0;
    }

    /**
     * <p>isDate.</p>
     *
     * @return a boolean.
     */
    public boolean isDate()
    {
        return false;
    }

    /**
     * <p>isFloat.</p>
     *
     * @return a boolean.
     */
    public boolean isFloat()
    {
        return false;
    }

    /**
     * <p>isInteger.</p>
     *
     * @return a boolean.
     */
    public boolean isInteger()
    {
        return false;
    }

    /**
     * <p>isNumeric.</p>
     *
     * @return a boolean.
     */
    abstract public boolean isNumeric();

    /**
     * <p>newCopy.</p>
     *
     * @return a {@link com.obdobion.funnel.orderby.KeyPart} object.
     */
    public KeyPart newCopy()
    {
        KeyPart myCopy;
        try
        {
            myCopy = getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e)
        {
            e.printStackTrace();
            return null;
        }
        myCopy.csvFieldNumber = csvFieldNumber;
        myCopy.offset = offset;
        myCopy.length = length;
        myCopy.parseFormat = parseFormat;
        myCopy.typeName = typeName;
        myCopy.direction = direction;
        myCopy.columnName = columnName;
        return myCopy;
    }

    /**
     * <p>originalData.</p>
     *
     * @param context a {@link com.obdobion.funnel.orderby.KeyContext} object.
     * @param proxyRecord a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     * @param outputBytes a {@link java.io.ByteArrayOutputStream} object.
     */
    public void originalData(final KeyContext context, final SourceProxyRecord proxyRecord,
            final ByteArrayOutputStream outputBytes)
    {
        final byte[] rawBytes = rawBytes(context);

        int lengthThisTime = length;
        if (proxyRecord.originalSize < offset + length)
            lengthThisTime = proxyRecord.originalSize - offset;

        outputBytes.write(rawBytes, offset, lengthThisTime);
        if (context.keyLength > lengthThisTime)
        {
            for (int x = 0; x < context.keyLength - lengthThisTime; x++)
                outputBytes.write(' ');
        }

        if (nextPart != null)
            nextPart.originalData(context, proxyRecord, outputBytes);
    }

    /**
     * <p>pack.</p>
     *
     * @param context a {@link com.obdobion.funnel.orderby.KeyContext} object.
     * @throws java.lang.Exception if any.
     */
    abstract public void pack(KeyContext context) throws Exception;

    /**
     * <p>parseObject.</p>
     *
     * @param context a {@link com.obdobion.funnel.orderby.KeyContext} object.
     * @throws java.lang.Exception if any.
     */
    public void parseObject(final KeyContext context) throws Exception
    {
        parseObjectFromRawData(rawBytes(context));
    }

    /**
     * <p>parseObjectFromRawData.</p>
     *
     * @param rawData an array of byte.
     * @throws java.lang.Exception if any.
     */
    public abstract void parseObjectFromRawData(byte[] rawData) throws Exception;

    byte[] rawBytes(final KeyContext context)
    {
        if (csvFieldNumber >= 0)
            return context.rawRecordBytes[csvFieldNumber];
        return context.rawRecordBytes[0];
    }
}
