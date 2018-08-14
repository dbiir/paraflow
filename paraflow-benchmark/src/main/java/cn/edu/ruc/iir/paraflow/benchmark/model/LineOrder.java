package cn.edu.ruc.iir.paraflow.benchmark.model;

/**
 * paraflow
 *
 * @author guodong
 */
public class LineOrder
        implements Model
{
    private final long rowNumber;
    private final long lineOrderKey;
    private final long customerKey;
    private final char orderStatus;
    private final long totalPrice;
    private final int orderDate;
    private final String orderPriority;
    private final String clerk;
    private final int shipPriority;
    private final String orderComment;
    private final int lineNumber;
    private final long quantity;
    private final long extendedPrice;
    private final long discount;
    private final long tax;
    private final String returnFlag;
    private final char lineStatus;
    private final int shipDate;
    private final int commitDate;
    private final int receiptDate;
    private final String shipInstructions;
    private final String shipMode;
    private final String lineitemComment;
    private final long creation;

    public LineOrder(
            long rowNumber,
            long lineOrderKey,
            long customerKey,
            char orderStatus,
            String clerk,
            String orderComment,
            String lineitemComment,
            long quantity,
            long discount,
            long tax,
            int shipDate,
            int commitDate,
            int receiptDate,
            String returnFlag,
            String shipInstructions,
            String shipMode,
            long totalPrice,
            int orderDate,
            String orderPriority,
            int shipPriority,
            int lineNumber,
            long extendedPrice,
            char lineStatus,
            long creation)
    {
        this.rowNumber = rowNumber;
        this.lineOrderKey = lineOrderKey;
        this.customerKey = customerKey;
        this.orderStatus = orderStatus;
        this.totalPrice = totalPrice;
        this.orderDate = orderDate;
        this.orderPriority = orderPriority;
        this.clerk = clerk;
        this.shipPriority = shipPriority;
        this.orderComment = orderComment;
        this.lineNumber = lineNumber;
        this.quantity = quantity;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
        this.tax = tax;
        this.returnFlag = returnFlag;
        this.lineStatus = lineStatus;
        this.shipDate = shipDate;
        this.commitDate = commitDate;
        this.receiptDate = receiptDate;
        this.shipInstructions = shipInstructions;
        this.shipMode = shipMode;
        this.lineitemComment = lineitemComment;
        this.creation = creation;
    }

    public long getLineOrderKey()
    {
        return lineOrderKey;
    }

    public long getCustomerKey()
    {
        return customerKey;
    }

    public char getOrderStatus()
    {
        return orderStatus;
    }

    public double getTotalPrice()
    {
        return totalPrice / 100.0;
    }

    public long getTotalPriceInCents()
    {
        return totalPrice;
    }

    public int getOrderDate()
    {
        return orderDate;
    }

    public String getOrderPriority()
    {
        return orderPriority;
    }

    public String getClerk()
    {
        return clerk;
    }

    public int getShipPriority()
    {
        return shipPriority;
    }

    public String getOrderComment()
    {
        return orderComment;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public long getQuantity()
    {
        return quantity;
    }

    public double getExtendedPrice()
    {
        return extendedPrice / 100.0;
    }

    public long getExtendedPriceInCents()
    {
        return extendedPrice;
    }

    public double getDiscount()
    {
        return discount / 100.0;
    }

    public long getDiscountPercent()
    {
        return discount;
    }

    public double getTax()
    {
        return tax / 100.0;
    }

    public long getTaxPercent()
    {
        return tax;
    }

    public String getReturnFlag()
    {
        return returnFlag;
    }

    public char getLineStatus()
    {
        return lineStatus;
    }

    public int getShipDate()
    {
        return shipDate;
    }

    public int getCommitDate()
    {
        return commitDate;
    }

    public int getReceiptDate()
    {
        return receiptDate;
    }

    public String getShipInstructions()
    {
        return shipInstructions;
    }

    public String getShipMode()
    {
        return shipMode;
    }

    public String getLineitemComment()
    {
        return lineitemComment;
    }

    public long getCreation()
    {
        return creation;
    }

    public long getRowNumber()
    {
        return rowNumber;
    }

    public String toLine()
    {
        return String.format("%d|%d|%s|%d|%d|%s|%s|%d|%s|%d|%d|%d|%d|%d|%s|%s|%d|%d|%d|%s|%s|%s|%d",
                customerKey, lineOrderKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority,
                orderComment, lineNumber, quantity, extendedPrice, discount, tax, returnFlag, lineStatus, shipDate,
                commitDate, receiptDate, shipInstructions, shipMode, lineitemComment, creation);
    }
}
