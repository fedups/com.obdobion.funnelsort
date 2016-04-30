package com.obdobion.funnel.columns;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 * 
 */
public class OutputFormatHelper
{
    final private static Logger logger       = Logger.getLogger(OutputFormatHelper.class);

    public static final int     MAX_OUTPUT_SIZE = 4096;
    final KeyContext            context;
    final int                   maxRecordBytes;
    final ColumnHelper          columnHelper;
    FormatPart                  formatter;
    List<FormatPart>            columns;

    public OutputFormatHelper(ColumnHelper _columnHelper)
    {
        this(_columnHelper, MAX_OUTPUT_SIZE);
    }

    public OutputFormatHelper(ColumnHelper _columnHelper, final int maxsize)
    {
        logger.debug("maximum output record length is " + MAX_OUTPUT_SIZE);

        maxRecordBytes = maxsize;
        context = new KeyContext();
        columns = new ArrayList<>();
        columnHelper = _columnHelper;
    }

    /**
     * Add the field in sequence after all other fields that have already been
     * defined. This is done through a linked list of fields. Use the column
     * helper to find the definition of the key if a column name was specified.
     * 
     * @param _formatter
     */
    public void add (final FormatPart _formatter)
    {
        if (columnHelper != null && columnHelper.exists(_formatter.columnName))
        {
            KeyPart colDef = columnHelper.get(_formatter.columnName);
            _formatter.defineFrom(colDef);
        }

        if (this.formatter == null)
            this.formatter = _formatter;
        else
            this.formatter.add(_formatter);
    }

    /**
     * It is likely that the provided data is a reusable buffer of bytes. So we
     * can't just store these bytes for later use.
     * 
     * @param data
     * @return
     * @throws Exception
     */
    KeyContext extract (final byte[] data, SourceProxyRecord proxyRecord) throws Exception
    {
        /*
         * The extra byte is for a 0x00 character to be placed at the end of
         * String keys. This is important in order to handle keys where the user
         * specified the maximum length for a String key. Or took the default
         * sort, which is the maximum key.
         */
        context.key = new byte[maxRecordBytes + 1];
        context.keyLength = 0;
        context.rawRecordBytes = new byte[1][];
        context.rawRecordBytes[0] = data;
        context.recordNumber = proxyRecord.originalRecordNumber;

        ByteArrayOutputStream output = new ByteArrayOutputStream(maxRecordBytes);
        formatter.originalData(context, proxyRecord, output);
        context.key = output.toByteArray();
        context.keyLength = context.key.length;

        context.rawRecordBytes = null;
        return context;
    }

    public void format (ColumnWriter writer, final byte[] originalData, SourceProxyRecord proxyRecord) throws Exception
    {
        if (formatter == null)
        {
            writer.write(originalData, 0, proxyRecord.originalSize);
            return;
        }
        /*
         * Use the output column definitions to format here. The real issue is
         * that the input computations and system variables are not available at
         * this point in the process. The rows from the original source were
         * read and a tag sort was performed. Now that the rows are ready for
         * writing it is necessary to recompute those fields.
         */
        extract(originalData, proxyRecord);

        writer.write(context.key, 0, context.keyLength);
    }

}