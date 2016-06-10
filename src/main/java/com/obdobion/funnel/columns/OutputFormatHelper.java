package com.obdobion.funnel.columns;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.algebrain.Equ;
import com.obdobion.funnel.aggregation.Aggregate;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
public class OutputFormatHelper
{
    final private static Logger logger          = LoggerFactory.getLogger(OutputFormatHelper.class);

    public static final int     MAX_OUTPUT_SIZE = 4096;

    public static int lengthToWrite (final byte[] data, final int offset, final int dataLength, final boolean rightTrim)
    {
        int lengthToWrite = 0;
        if (!rightTrim)
            lengthToWrite = dataLength;
        else
            for (int i = offset + dataLength - 1; i >= offset; i--)
                if (data[i] != ' ')
                {
                    lengthToWrite = (i - offset) + 1;
                    break;
                }
        return lengthToWrite;
    }

    final KeyContext   context;
    final int          maxRecordBytes;
    final ColumnHelper columnHelper;
    final HeaderHelper headerHelper;
    FormatPart         formatter;

    List<FormatPart>   columns;

    private Equ[]      referencesToAllOutputFormatEquations;

    public OutputFormatHelper(final ColumnHelper _columnHelper, final HeaderHelper _headerHelper)
    {
        this(_columnHelper, _headerHelper, MAX_OUTPUT_SIZE);
    }

    OutputFormatHelper(final ColumnHelper _columnHelper, final HeaderHelper _headerHelper, final int maxsize)
    {
        logger.debug("maximum output record length is {}", MAX_OUTPUT_SIZE);

        maxRecordBytes = maxsize;
        context = new KeyContext();
        columns = new ArrayList<>();
        columnHelper = _columnHelper;
        headerHelper = _headerHelper;
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
            final KeyPart colDef = columnHelper.get(_formatter.columnName);
            _formatter.defineFrom(colDef);
        }
        if (headerHelper != null && headerHelper.exists(_formatter.columnName))
        {
            final KeyPart colDef = headerHelper.get(_formatter.columnName);
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
    KeyContext extract (final FunnelContext funnelContext, final byte[] data, final SourceProxyRecord proxyRecord)
        throws Exception
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
        context.recordNumber = proxyRecord.getOriginalRecordNumber();

        final ByteArrayOutputStream output = new ByteArrayOutputStream(maxRecordBytes);
        /*
         * Use the output column definitions to format here. The real issue is
         * that the input computations and system variables are not available at
         * this point in the process. The rows from the original source were
         * read and a tag sort was performed. Now that the rows are ready for
         * writing it is necessary to recompute those fields.
         */
        prepareEquationsWithOriginalColumnData(funnelContext);
        formatter.originalData(context, funnelContext, proxyRecord.originalSize, output);

        context.key = output.toByteArray();
        context.keyLength = context.key.length;

        context.rawRecordBytes = null;
        return context;
    }

    public void format (
        final ColumnWriter writer,
        final byte[] originalData,
        final int dataSize,
        final SourceProxyRecord proxyRecord,
        final boolean rightTrim) throws Exception
    {
        if (formatter == null)
        {
            final int lengthToWrite = lengthToWrite(originalData, 0, dataSize, rightTrim);
            writer.write(originalData, 0, lengthToWrite);
            return;
        }
        extract(proxyRecord.getFunnelContext(), originalData, proxyRecord);

        final int lengthToWrite = lengthToWrite(context.key, 0, context.keyLength, rightTrim);
        writer.write(context.key, 0, lengthToWrite);
    }

    public Equ[] getReferencesToAllOutputFormatEquations (final FunnelContext funnelContext)
    {
        if (referencesToAllOutputFormatEquations == null)
        {
            /*
             * First count all of the equations so we can make an array.
             */
            int equationCount = 0;
            if (funnelContext.isAggregating())
            {
                /*
                 * Also count all of the aggregate equations because they are
                 * considered to be output functions.
                 */
                for (final Aggregate agg : funnelContext.aggregates)
                {
                    if (agg.equation != null)
                        equationCount++;
                }
            }
            if (funnelContext.formatOutDefs != null)
            {
                for (final FormatPart def : funnelContext.formatOutDefs)
                {
                    if (def.equation != null)
                        equationCount++;
                }
            }
            if (funnelContext.headerOutDefs != null)
            {
                for (final FormatPart def : funnelContext.headerOutDefs)
                {
                    if (def.equation != null)
                        equationCount++;
                }
            }
            referencesToAllOutputFormatEquations = new Equ[equationCount];

            equationCount = 0;
            if (funnelContext.isAggregating())
            {
                /*
                 * Also include all of the aggregate equations because they are
                 * considered to be output functions.
                 */
                for (final Aggregate agg : funnelContext.aggregates)
                {
                    if (agg.equation != null)
                        referencesToAllOutputFormatEquations[equationCount++] = agg.equation;
                }
            }
            if (funnelContext.formatOutDefs != null)
            {
                for (final FormatPart def : funnelContext.formatOutDefs)
                {
                    if (def.equation != null)
                        referencesToAllOutputFormatEquations[equationCount++] = def.equation;
                }
            }
            if (funnelContext.headerOutDefs != null)
            {
                for (final FormatPart def : funnelContext.headerOutDefs)
                {
                    if (def.equation != null)
                        referencesToAllOutputFormatEquations[equationCount++] = def.equation;
                }
            }
        }
        return referencesToAllOutputFormatEquations;
    }

    private void prepareEquationsWithOriginalColumnData (final FunnelContext funnelContext) throws Exception
    {
        /*
         * Cache the variable values into all related equations ahead of need.
         */
        getReferencesToAllOutputFormatEquations(funnelContext);
        /*
         * Just to get the variables in the equation loaded from the original
         * record. This loading the column values into the aggregate equations
         * too.
         */
        funnelContext.columnHelper.extract(
            funnelContext,
            context.rawRecordBytes[0],
            context.recordNumber,
            context.rawRecordBytes[0].length,
            referencesToAllOutputFormatEquations);
        /*
         * In order to get the aggregate values into the format equations they
         * will also be needlessly loaded back into the aggregate equations too.
         */
        Aggregate.loadValues(funnelContext, referencesToAllOutputFormatEquations);
    }

}