package cn.edu.ruc.iir.paraflow.benchmark.generator;

import cn.edu.ruc.iir.paraflow.benchmark.model.Region;
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
public class RegionGenerator
        implements Generator<Region>
{
    private static final int COMMENT_AVERAGE_LENGTH = 72;

    private final Distributions distributions;
    private final TextPool textPool;

    public RegionGenerator()
    {
        this(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public RegionGenerator(Distributions distributions, TextPool textPool)
    {
        this.distributions = Objects.requireNonNull(distributions, "distributions is null");
        this.textPool = Objects.requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Region> iterator()
    {
        return new RegionGeneratorIterator(distributions.getRegions(), textPool);
    }

    private static class RegionGeneratorIterator
            extends AbstractIterator<Region>
    {
        private final Distribution regions;
        private final RandomText commentRandom;

        private int index;

        private RegionGeneratorIterator(Distribution regions, TextPool textPool)
        {
            this.regions = regions;
            this.commentRandom = new RandomText(1500869201, textPool, COMMENT_AVERAGE_LENGTH);
        }

        @Override
        protected Region computeNext()
        {
            if (index >= regions.size()) {
                return endOfData();
            }

            Region region = new Region(index,
                    index,
                    regions.getValue(index),
                    commentRandom.nextValue());

            commentRandom.rowFinished();
            index++;

            return region;
        }
    }
}
