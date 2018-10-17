package cn.edu.ruc.iir.paraflow.benchmark.generator;

import cn.edu.ruc.iir.paraflow.benchmark.model.Nation;
import com.google.common.collect.AbstractIterator;
import io.airlift.tpch.Distribution;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

import java.util.Iterator;
import java.util.Objects;

/**
 * paraflow
 *
 * @author guodong
 */
public class NationGenerator
        implements Generator<Nation>
{
    private static final int COMMENT_AVERAGE_LENGTH = 72;

    private final Distributions distributions;
    private final TextPool textPool;

    public NationGenerator()
    {
        this(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    NationGenerator(Distributions distributions, TextPool textPool)
    {
        this.distributions = Objects.requireNonNull(distributions, "distributions is null");
        this.textPool = Objects.requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Nation> iterator()
    {
        return new NationGeneratorIterator(distributions.getNations(), textPool);
    }

    private static class NationGeneratorIterator
            extends AbstractIterator<Nation>
    {
        private final Distribution nations;
        private final RandomText commentRandom;

        private int index;

        private NationGeneratorIterator(Distribution nations, TextPool textPool)
        {
            this.nations = nations;
            this.commentRandom = new RandomText(606179079, textPool, COMMENT_AVERAGE_LENGTH);
        }

        @Override
        protected Nation computeNext()
        {
            if (index >= nations.size()) {
                return endOfData();
            }

            Nation nation = new Nation(index,
                    index,
                    nations.getValue(index),
                    nations.getWeight(index),
                    commentRandom.nextValue());

            commentRandom.rowFinished();
            index++;

            return nation;
        }
    }
}
