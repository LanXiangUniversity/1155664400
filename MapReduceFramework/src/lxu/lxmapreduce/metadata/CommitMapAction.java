package lxu.lxmapreduce.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/18.
 */
public class CommitMapAction extends TaskTrackerAction implements Serializable {
    public CommitMapAction(ActionType actionType) {
        super(ActionType.COMMIT_TASK);
    }
}
