package cn.edu.ruc.iir.paraflow.benchmark.query;

import java.util.Random;

/**
 * paraflow db query generator.
 * based on the given distribution, generate queries for db.
 * | select | insert | update | delete |
 * -----------------------------------
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
    private final long range;
    private final long selectFloor;
    private final long insertFloor;
    private final long updateFloor;
    private final long deleteFloor;

    public DBQueryGenerator(QueryDistribution distribution, String table)
    {
        super(distribution);
        this.selectTemplate = new DBSelectTemplate(table);
        this.insertTemplate = new DBInsertTemplate(table);
        this.updateTemplate = new DBUpdateTemplate(table);
        this.deleteTemplate = new DBDeleteTemplate(table);
        selectFloor = 0;
        insertFloor = distribution.getValue("select") + selectFloor;
        updateFloor = distribution.getValue("insert") + insertFloor;
        deleteFloor = distribution.getValue("update") + updateFloor;
        range = distribution.getValue("delete") + deleteFloor;
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
        long randomV = random.nextInt((int) range);
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
