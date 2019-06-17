package cn.edu.ruc.iir.paraflow.benchmark.model;

import java.util.Objects;

import static io.airlift.tpch.GenerateUtils.formatMoney;

/**
 * paraflow
 *
 * @author guodong
 */
public class Customer
        implements Model
{
    private final long rowNumber;
    private final long customerKey;
    private final String name;
    private final String address;
    private final long nationKey;
    private final String phone;
    private final long accountBalance;
    private final String marketSegment;
    private final String comment;

    public Customer(long rowNumber, long customerKey, String name, String address, long nationKey,
                    String phone, long accountBalance, String marketSegment, String comment)
    {
        this.rowNumber = rowNumber;
        this.customerKey = customerKey;
        this.name = Objects.requireNonNull(name, "name is null");
        this.address = Objects.requireNonNull(address, "address is null");
        this.nationKey = nationKey;
        this.phone = Objects.requireNonNull(phone, "phone is null");
        this.accountBalance = accountBalance;
        this.marketSegment = Objects.requireNonNull(marketSegment, "marketSegment is null");
        this.comment = Objects.requireNonNull(comment, "comment is null");
    }

    @Override
    public long getRowNumber()
    {
        return rowNumber;
    }

    @Override
    public String toLine()
    {
        return customerKey + "|" +
                name + "|" +
                address + "|" +
                nationKey + "|" +
                phone + "|" +
                formatMoney(accountBalance) + "|" +
                marketSegment + "|" +
                comment;
    }

    public long getCustomerKey()
    {
        return customerKey;
    }

    public String getName()
    {
        return name;
    }

    public String getAddress()
    {
        return address;
    }

    public long getNationKey()
    {
        return nationKey;
    }

    public String getPhone()
    {
        return phone;
    }

    public double getAccountBalance()
    {
        return accountBalance / 100.0;
    }

    public long getAccountBalanceInCents()
    {
        return accountBalance;
    }

    public String getMarketSegment()
    {
        return marketSegment;
    }

    public String getComment()
    {
        return comment;
    }
}
