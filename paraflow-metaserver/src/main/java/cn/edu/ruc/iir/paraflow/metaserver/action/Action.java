package cn.edu.ruc.iir.paraflow.metaserver.action;

import java.util.ArrayList;
import java.util.List;

/**
 * paraflow
 *
 * @author guodong
 */
public abstract class Action
{
    private final List<Action> dependentActions;

    public Action()
    {
        dependentActions = new ArrayList<>();
    }

    public void run()
    {
        for (Action action : dependentActions) {
            action.run();
        }
        act();
    }

    abstract void act();

    public void addDependentAction(Action action)
    {
        dependentActions.add(action);
    }
}
