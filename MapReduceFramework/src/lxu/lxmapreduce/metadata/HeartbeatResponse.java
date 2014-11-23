package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.configuration.Configuration;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by magl on 14/11/10.
 */
public class HeartbeatResponse implements Serializable {
    private Configuration configuration;
    private short responseID;
    private String trackerName;
    private ArrayList<TaskTrackerAction> actions;

    public HeartbeatResponse(short responseID,
                             Configuration configuration,
                             ArrayList<TaskTrackerAction> actions) {
        this.responseID = responseID;
        this.configuration = configuration;
        this.actions = actions;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public short getResponseID() {
        return responseID;
    }

    public void setResponseID(short responseID) {
        this.responseID = responseID;
    }

    public String getTrackerName() {
        return trackerName;
    }

    public void setTrackerName(String trackerName) {
        this.trackerName = trackerName;
    }

    public ArrayList<TaskTrackerAction> getActions() {
        return actions;
    }

    public void setActions(ArrayList<TaskTrackerAction> actions) {
        this.actions = actions;
    }
}
