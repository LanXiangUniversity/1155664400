package lxu.lxmapreduce.metadata;

import java.io.Serializable;

/**
 * Created by magl on 14/11/18.
 */
public class CommitMapAction extends TaskTrackerAction implements Serializable {
    private String jobID;
    public CommitMapAction(String jobID, ActionType actionType) {
        super(ActionType.COMMIT_TASK);
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }
}
