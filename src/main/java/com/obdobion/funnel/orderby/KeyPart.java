package com.obdobion.funnel.orderby;

import java.io.ByteArrayOutputStream;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
abstract public class KeyPart
{
    public int          csvFieldNumber;
    public int          offset;
    public int          length;
    public KeyDirection direction;
    public String       parseFormat;
    public KeyPart      nextPart;
    public KeyType      typeName;
    public String       columnName;
    byte[]              unformattedContents;

    public KeyPart()
    {
        super();
        csvFieldNumber = -1;
        typeName = null;
        columnName = null;
        offset = -1;
    }

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
     * @param colDef
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

    abstract public Object getContents();

    public byte[] getContentsAsByteArray()
    {
        return unformattedContents;
    }

    abstract public double getContentsAsDouble();

    public boolean isCsv()
    {
        return csvFieldNumber >= 0;
    }

    public boolean isDate()
    {
        return false;
    }

    public boolean isFloat()
    {
        return false;
    }

    public boolean isInteger()
    {
        return false;
    }

    abstract public boolean isNumeric();

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

    abstract public void pack(KeyContext context) throws Exception;

    public void parseObject(final KeyContext context) throws Exception
    {
        parseObjectFromRawData(rawBytes(context));
    }

    public abstract void parseObjectFromRawData(byte[] rawData) throws Exception;

    byte[] rawBytes(final KeyContext context)
    {
        if (csvFieldNumber >= 0)
            return context.rawRecordBytes[csvFieldNumber];
        return context.rawRecordBytes[0];
    }
}