package cn.edu.ruc.iir.paraflow.benchmark.model;

import cn.edu.ruc.iir.paraflow.commons.ParaflowRecord;

/**
 * paraflow
 *
 * @author guodong
 */
public class LineOrder
        extends ParaflowRecord
        implements Model
{
    private long rowNumber;
    private Object[] values;

    public LineOrder()
    {
        this.values = new Object[23];
    }

    public LineOrder(
            long rowNumber,
            long lineOrderKey,
            long customerKey,
            byte[] orderStatus,
            double totalPrice,
            int orderDate,
            byte[] orderPriority,
            byte[] clerk,
            int shipPriority,
            byte[] orderComment,
            int lineNumber,
            double quantity,
            double extendedPrice,
            double discount,
            double tax,
            byte[] returnFlag,
            byte[] lineStatus,
            int shipDate,
            int commitDate,
            int receiptDate,
            byte[] shipInstructions,
            byte[] shipMode,
            byte[] lineitemComment,
            long creation)
    {
        this.rowNumber = rowNumber;
        this.values = new Object[]{lineOrderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                clerk, shipPriority, orderComment, lineNumber, quantity / 100.0d, extendedPrice / 100.0d, discount / 100.0d,
                tax / 100.0d, returnFlag, lineStatus, shipDate, commitDate, receiptDate, shipInstructions, shipMode,
                lineitemComment, creation};
    }

    @Override
    public Object getValue(int idx)
    {
        return values[idx];
    }

    public long getLineOrderKey()
    {
        return (long) this.values[0];
    }

    public void setLineOrderKey(long lineOrderKey)
    {
        this.values[0] = lineOrderKey;
    }

    public long getCustomerKey()
    {
        return (long) values[1];
    }

    public void setCustomerKey(long customerKey)
    {
        this.values[1] = customerKey;
    }

    public char getOrderStatus()
    {
        return (char) values[2];
    }

    public void setOrderStatus(char orderStatus)
    {
        this.values[2] = orderStatus;
    }

    public double getTotalPrice()
    {
        return (double) values[3];
    }

    public void setTotalPrice(double totalPrice)
    {
        this.values[3] = totalPrice;
    }

    public long getTotalPriceInCents()
    {
        return (long) values[3];
    }

    public int getOrderDate()
    {
        return (int) values[4];
    }

    public void setOrderDate(int orderDate)
    {
        this.values[4] = orderDate;
    }

    public String getOrderPriority()
    {
        return (String) values[5];
    }

    public void setOrderPriority(String orderPriority)
    {
        this.values[5] = orderPriority;
    }

    public String getClerk()
    {
        return (String) values[6];
    }

    public void setClerk(String clerk)
    {
        this.values[6] = clerk;
    }

    public int getShipPriority()
    {
        return (int) values[7];
    }

    public void setShipPriority(int shipPriority)
    {
        this.values[7] = shipPriority;
    }

    public String getOrderComment()
    {
        return (String) values[8];
    }

    public void setOrderComment(String orderComment)
    {
        this.values[8] = orderComment;
    }

    public int getLineNumber()
    {
        return (int) values[9];
    }

    public void setLineNumber(int lineNumber)
    {
        this.values[9] = lineNumber;
    }

    public double getQuantity()
    {
        return (double) values[10];
    }

    public void setQuantity(double quantity)
    {
        this.values[10] = quantity;
    }

    public double getExtendedPrice()
    {
        return (double) values[11];
    }

    public void setExtendedPrice(double extendedPrice)
    {
        this.values[11] = extendedPrice;
    }

    public double getDiscount()
    {
        return (double) values[12];
    }

    public void setDiscount(double discount)
    {
        this.values[12] = discount;
    }

    public double getTax()
    {
        return (double) values[13];
    }

    public void setTax(double tax)
    {
        this.values[13] = tax;
    }

    public String getReturnFlag()
    {
        return (String) values[14];
    }

    public void setReturnFlag(String returnFlag)
    {
        this.values[14] = returnFlag;
    }

    public char getLineStatus()
    {
        return (char) values[15];
    }

    public void setLineStatus(char lineStatus)
    {
        this.values[15] = lineStatus;
    }

    public int getShipDate()
    {
        return (int) values[16];
    }

    public void setShipDate(int shipDate)
    {
        this.values[16] = shipDate;
    }

    public int getCommitDate()
    {
        return (int) values[17];
    }

    public void setCommitDate(int commitDate)
    {
        this.values[17] = commitDate;
    }

    public int getReceiptDate()
    {
        return (int) values[18];
    }

    public void setReceiptDate(int receiptDate)
    {
        this.values[18] = receiptDate;
    }

    public String getShipInstructions()
    {
        return (String) values[19];
    }

    public void setShipInstructions(String shipInstructions)
    {
        this.values[19] = shipInstructions;
    }

    public String getShipMode()
    {
        return (String) values[20];
    }

    public void setShipMode(String shipMode)
    {
        this.values[20] = shipMode;
    }

    public String getLineitemComment()
    {
        return (String) values[21];
    }

    public void setLineitemComment(String lineitemComment)
    {
        this.values[21] = lineitemComment;
    }

    public long getCreation()
    {
        return (long) values[22];
    }

    public void setCreation(long creation)
    {
        this.values[22] = creation;
    }

    public long getRowNumber()
    {
        return rowNumber;
    }

    public void setRowNumber(long rowNumber)
    {
        this.rowNumber = rowNumber;
    }

    public String toLine()
    {
        return "";
    }
}
