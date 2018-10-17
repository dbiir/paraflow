package cn.edu.ruc.iir.paraflow.benchmark.generator;

import cn.edu.ruc.iir.paraflow.benchmark.model.Customer;
import com.google.common.collect.AbstractIterator;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.RandomAlphaNumeric;
import io.airlift.tpch.RandomBoundedInt;
import io.airlift.tpch.RandomPhoneNumber;
import io.airlift.tpch.RandomString;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import java.util.Iterator;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.tpch.GenerateUtils.calculateRowCount;
import static io.airlift.tpch.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;

/**
 * paraflow
 *
 * @author guodong
 */
public class CustomerGenerator
        implements Generator<Customer>
{
    static final int SCALE_BASE = 150_000;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 73;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public CustomerGenerator(double scaleFactor, int part, int partCount)
    {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public CustomerGenerator(double scaleFactor, int part, int partCount, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = Objects.requireNonNull(distributions, "distributions is null");
        this.textPool = Objects.requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Customer> iterator()
    {
        return new CustomerGeneratorIterator(
                distributions,
                textPool,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class CustomerGeneratorIterator
            extends AbstractIterator<Customer>
    {
        private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
        private final RandomBoundedInt nationKeyRandom;
        private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
        private final RandomBoundedInt accountBalanceRandom = new RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
        private final RandomString marketSegmentRandom;
        private final RandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long index;

        private CustomerGeneratorIterator(Distributions distributions, TextPool textPool, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            nationKeyRandom = new RandomBoundedInt(1489529863, 0, distributions.getNations().size() - 1);
            marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
            commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

            addressRandom.advanceRows(startIndex);
            nationKeyRandom.advanceRows(startIndex);
            phoneRandom.advanceRows(startIndex);
            accountBalanceRandom.advanceRows(startIndex);
            marketSegmentRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        protected Customer computeNext()
        {
            if (index >= rowCount) {
                return endOfData();
            }

            Customer customer = makeCustomer(startIndex + index + 1);

            addressRandom.rowFinished();
            nationKeyRandom.rowFinished();
            phoneRandom.rowFinished();
            accountBalanceRandom.rowFinished();
            marketSegmentRandom.rowFinished();
            commentRandom.rowFinished();

            index++;

            return customer;
        }

        private Customer makeCustomer(long customerKey)
        {
            long nationKey = nationKeyRandom.nextValue();

            return new Customer(customerKey,
                    customerKey,
                    String.format(ENGLISH, "Customer#%09d", customerKey),
                    addressRandom.nextValue(),
                    nationKey,
                    phoneRandom.nextValue(nationKey),
                    accountBalanceRandom.nextValue(),
                    marketSegmentRandom.nextValue(),
                    commentRandom.nextValue());
        }
    }
}
