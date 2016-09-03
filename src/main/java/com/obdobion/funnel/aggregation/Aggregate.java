package com.obdobion.funnel.aggregation;

import com.obdobion.algebrain.Equ;
import com.obdobion.argument.annotation.Arg;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * Abstract Aggregate class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
abstract public class Aggregate
{
    /**
     * <p>
     * aggregate.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @param originalRecordSize a int.
     * @param originalRecordNumber a long.
     * @throws java.lang.Exception if any.
     */
    public static void aggregate(final FunnelContext context, final int originalRecordSize,
            final long originalRecordNumber) throws Exception
    {
        if (context.isAggregating())
        {
            loadColumnsIntoAggregateEquations(context, originalRecordSize, originalRecordNumber);
            for (final Aggregate agg : context.getAggregates())
            {
                agg.update(context);
            }
        }
    }

    private static void loadColumnsIntoAggregateEquations(final FunnelContext context, final int originalRecordSize,
            final long originalRecordNumber) throws Exception
    {
        for (final Aggregate agg : context.getAggregates())
        {
            if (agg.equation != null)
            {
                for (final KeyPart col : context.columnHelper.getColumns())
                    agg.equation.getSupport().assignVariable(col.columnName, col.getContents());

                agg.equation.getSupport().assignVariable(Funnel.SYS_RECORDNUMBER, new Long(originalRecordNumber));
                agg.equation.getSupport().assignVariable(Funnel.SYS_RECORDSIZE, new Long(originalRecordSize));
            }
        }
    }

    /**
     * <p>
     * loadValues.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @param referencesToAllOutputFormatEquations an array of
     *            {@link com.obdobion.algebrain.Equ} objects.
     * @throws java.lang.Exception if any.
     */
    public static void loadValues(final FunnelContext context, final Equ[] referencesToAllOutputFormatEquations)
            throws Exception
    {
        if (context.isAggregating())
            for (final Aggregate agg : context.getAggregates())
            {
                for (final Equ equ : referencesToAllOutputFormatEquations)
                {
                    equ.getSupport().assignVariable(agg.name, agg.getValueForEquations());
                }
            }
    }

    /**
     * <p>
     * newAvg.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    static public Aggregate newAvg()
    {
        return new AggregateAvg();
    }

    /**
     * <p>
     * newCount.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    static public Aggregate newCount()
    {
        return new AggregateCount();
    }

    /**
     * <p>
     * newMax.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    static public Aggregate newMax()
    {
        return new AggregateMax();
    }

    /**
     * <p>
     * newMin.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    static public Aggregate newMin()
    {
        return new AggregateMin();
    }

    /**
     * <p>
     * newSum.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.aggregation.Aggregate} object.
     */
    static public Aggregate newSum()
    {
        return new AggregateSum();
    }

    /**
     * <p>
     * reset.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     */
    public static void reset(final FunnelContext context)
    {
        if (context.isAggregating())
            for (final Aggregate agg : context.getAggregates())
            {
                agg.reset();
            }
    }

    @Arg(positional = true, allowCamelCaps = true, help = "A previously defined column name.")
    public String columnName;

    @Arg(shortName = 'n', required = true, help = "A name for this aggregate so that it can be referenced.")
    public String name;

    @Arg(shortName = 'e', allowMetaphone = true, help = "Used instead of a column name.")
    public Equ    equation;

    AggType       aggType;

    abstract Object getValueForEquations();

    abstract void reset();

    /**
     * <p>
     * supportsDate.
     * </p>
     *
     * @return a boolean.
     */
    public boolean supportsDate()
    {
        return true;
    }

    /**
     * <p>
     * supportsNumber.
     * </p>
     *
     * @return a boolean.
     */
    public boolean supportsNumber()
    {
        return true;
    }

    abstract void update(FunnelContext context) throws Exception;
}
