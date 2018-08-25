package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow db query generator.
 * based on the given distribution, generate queries for db.
 * | select | insert | update | delete |
 *  -----------------------------------
 *
 * @author guodong
 */
public class DBQueryGenerator
        extends QueryGenerator
{
    private final long startTime;
    private final DBSelectTemplate selectTemplate;
    private final DBInsertTemplate insertTemplate;
    private final DBUpdateTemplate updateTemplate;
    private final DBDeleteTemplate deleteTemplate;
    private final Random random;
    private final int range;
    private final int selectFloor;
    private final int insertFloor;
    private final int updateFloor;
    private final int deleteFloor;

    public DBQueryGenerator(QueryDistribution distribution)
    {
        super(distribution);
        this.selectTemplate = new DBSelectTemplate();
        this.insertTemplate = new DBInsertTemplate();
        this.updateTemplate = new DBUpdateTemplate();
        this.deleteTemplate = new DBDeleteTemplate();
        selectFloor = 0;
        insertFloor = distribution.selectValue() + selectFloor;
        updateFloor = distribution.insertValue() + insertFloor;
        deleteFloor = distribution.updateValue() + updateFloor;
        range = distribution.deleteValue() + deleteFloor;
        this.random = new Random();
        this.startTime = System.currentTimeMillis();
    }

    @Override
    public boolean hasNext()
    {
        if (distribution.timeLimit() >= 0) {
            return (System.currentTimeMillis() - startTime) < distribution.timeLimit();
        }
        return true;
    }

    @Override
    public String next()
    {
        int randomV = random.nextInt(range);
        if (randomV >= deleteFloor) {
            return deleteTemplate.makeQuery();
        }
        if (randomV >= updateFloor) {
            return updateTemplate.makeQuery();
        }
        if (randomV >= insertFloor) {
            return insertTemplate.makeQuery();
        }
        if (randomV >= selectFloor) {
            return selectTemplate.makeQuery();
        }
        return null;
    }
}
