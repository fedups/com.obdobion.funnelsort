package com.obdobion.funnel.columns;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.algebrain.Equ;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>ColumnHelper class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class ColumnHelper
{
    final private static Logger logger          = LoggerFactory.getLogger(ColumnHelper.class);

    /** Constant <code>MAX_COLUMN_SIZE=255</code> */
    public static final int     MAX_COLUMN_SIZE = 255;
    final KeyContext            context;
    final int                   maxKeyBytes;
    List<KeyPart>               columns;

    /**
     * <p>Constructor for ColumnHelper.</p>
     */
    public ColumnHelper()
    {
        this(MAX_COLUMN_SIZE);
    }

    /**
     * <p>Constructor for ColumnHelper.</p>
     *
     * @param maxsize a int.
     */
    public ColumnHelper(final int maxsize)
    {
        logger.debug("maximum column length is {}", MAX_COLUMN_SIZE);

        maxKeyBytes = maxsize;
        context = new KeyContext();
        columns = new ArrayList<>();
    }

    /**
     * <p>add.</p>
     *
     * @param formatter a {@link com.obdobion.funnel.orderby.KeyPart} object.
     * @throws java.text.ParseException if any.
     */
    public void add (final KeyPart formatter) throws ParseException
    {
        if (exists(formatter.columnName))
            throw new ParseException("Column already defined: " + formatter.columnName, 0);
        final KeyPart myCopy = formatter.newCopy();
        columns.add(myCopy);
    }

    /**
     * <p>exists.</p>
     *
     * @param name a {@link java.lang.String} object.
     * @return a boolean.
     */
    public boolean exists (final String name)
    {
        for (final KeyPart col : columns)
        {
            if (name == null)
            {
                if (col.columnName == null)
                    return true;
                continue;
            } else if (col.columnName == null)
                continue;

            if (col.columnName.equalsIgnoreCase(name))
                return true;
        }
        return false;
    }

    /**
     * It is likely that the provided data is a reusable buffer of bytes. So we
     * can't just store these bytes for later use.
     *
     * @param funnelContext a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @param data an array of byte.
     * @param recordNumber a long.
     * @param dataLength a int.
     * @param equations a {@link com.obdobion.algebrain.Equ} object.
     * @return a {@link com.obdobion.funnel.orderby.KeyContext} object.
     * @throws java.lang.Exception if any.
     */
    public KeyContext extract (
        final FunnelContext funnelContext,
        final byte[] data,
        final long recordNumber,
        final int dataLength,
        final Equ... equations)
        throws Exception
    {
        /*
         * The extra byte is for a 0x00 character to be placed at the end of
         * String keys. This is important in order to handle keys where the user
         * specified the maximum length for a String key. Or took the default
         * sort, which is the maximum key.
         */
        context.key = new byte[maxKeyBytes + 1];
        context.keyLength = 0;
        context.rawRecordBytes = new byte[1][];
        context.rawRecordBytes[0] = data;
        context.recordNumber = recordNumber;

        extractColumnContentsFromRawData(funnelContext, recordNumber, dataLength, equations);

        context.rawRecordBytes = null;
        return context;
    }

    /**
     * Call this method for csv files that break each row up into fields (byte
     * arrays). [][].
     *
     * @param data an array of byte.
     * @param recordNumber a long.
     * @throws java.lang.Exception if any.
     * @param funnelContext a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @param dataLength a int.
     * @param equations a {@link com.obdobion.algebrain.Equ} object.
     * @return a {@link com.obdobion.funnel.orderby.KeyContext} object.
     */
    public KeyContext extract (
        final FunnelContext funnelContext,
        final byte[][] data,
        final long recordNumber,
        final int dataLength,
        final Equ... equations) throws Exception
    {
        /*
         * The extra byte is for a 0x00 character to be placed at the end of
         * String keys. This is important in order to handle keys where the user
         * specified the maximum length for a String key. Or took the default
         * sort, which is the maximum key.
         */
        context.key = new byte[maxKeyBytes + 1];
        context.keyLength = 0;
        context.rawRecordBytes = data;
        context.recordNumber = recordNumber;

        extractColumnContentsFromRawData(funnelContext, recordNumber, dataLength, equations);

        context.rawRecordBytes = null;
        return context;
    }

    private void extractColumnContentsFromRawData (
        final FunnelContext funnelContext,
        final long recordNumber,
        final int dataLength,
        final Equ... equations)
        throws Exception
    {
        if (funnelContext == null)
            return;
        for (final KeyPart col : columns)
        {
            try
            {
                col.parseObject(context);
                for (int e = 0; e < equations.length; e++)
                {
                    if (equations[e] != null)
                        equations[e].getSupport().assignVariable(col.columnName, col.getContents());
                }

            } catch (final Exception e)
            {
                logger.warn("\"{}\" {} {} on record number {}",
                    col.columnName,
                    e.getClass().getSimpleName(),
                    e.getMessage(),
                    (recordNumber));
            }
        }
        final Long rn = new Long(recordNumber);
        final Long rs = new Long(dataLength);

        for (int e = 0; e < equations.length; e++)
        {
            if (equations[e] != null)
            {
                equations[e].getSupport().assignVariable(Funnel.SYS_RECORDNUMBER, rn);
                equations[e].getSupport().assignVariable(Funnel.SYS_RECORDSIZE, rs);
            }
        }
    }

    /**
     * <p>get.</p>
     *
     * @param name a {@link java.lang.String} object.
     * @return a {@link com.obdobion.funnel.orderby.KeyPart} object.
     */
    public KeyPart get (final String name)
    {
        for (final KeyPart col : columns)
        {
            if (name == null)
            {
                if (col.columnName == null)
                    return col;
                continue;
            } else if (col.columnName == null)
                continue;

            if (col.columnName.equalsIgnoreCase(name))
                return col;
        }
        return null;
    }

    /**
     * <p>Getter for the field <code>columns</code>.</p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<KeyPart> getColumns ()
    {
        return columns;
    }

    /**
     * <p>getNames.</p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<String> getNames ()
    {
        final List<String> allNames = new ArrayList<>();
        for (final KeyPart col : columns)
        {
            allNames.add(col.columnName);
        }
        return allNames;
    }

    /**
     * <p>loadColumnsFromBytes.</p>
     *
     * @param dataLength a long.
     * @param data an array of byte.
     * @param recordNumber a long.
     */
    public void loadColumnsFromBytes (final byte[] data, final long dataLength, final long recordNumber)
    {
        context.key = null;
        context.keyLength = 0;
        context.rawRecordBytes = new byte[1][];
        context.rawRecordBytes[0] = data;
        context.recordNumber = recordNumber;

        for (final KeyPart col : columns)
        {
            try
            {
                col.parseObject(context);

            } catch (final Exception e)
            {
                logger.warn("\"{}\" {} {} on record number {}",
                    col.columnName,
                    e.getClass().getSimpleName(),
                    e.getMessage(),
                    recordNumber);
            }
        }
    }
}
