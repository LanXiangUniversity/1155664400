package lxu.lxmapreduce.metadata;

import lxu.lxmapreduce.task.TaskStatus;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by magl on 14/11/10.
 */
public class TaskTrackerStatus implements Serializable {
    private String trackerName;
    private String hostIP;
    private long lastSeen;
    // Task Status List
    private LinkedList<TaskStatus> taskReports;
    private TaskTrackerHealthStatus healthStatus;

    private int maxMapTasks;
    private int maxReduceTasks;

    public String getTrackerName() {
        return trackerName;
    }

    public void setTrackerName(String trackerName) {
        this.trackerName = trackerName;
    }

    public String getHostIP() {
        return hostIP;
    }

    public void setHostIP(String hostIP) {
        this.hostIP = hostIP;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    public LinkedList<TaskStatus> getTaskReports() {
        return taskReports;
    }

    public void setTaskReports(LinkedList<TaskStatus> taskReports) {
        this.taskReports = taskReports;
    }

    public int getMaxMapTasks() {
        return maxMapTasks;
    }

    public void setMaxMapTasks(int maxMapTasks) {
        this.maxMapTasks = maxMapTasks;
    }

    public int getMaxReduceTasks() {
        return maxReduceTasks;
    }

    public void setMaxReduceTasks(int maxReduceTasks) {
        this.maxReduceTasks = maxReduceTasks;
    }

    public TaskTrackerHealthStatus getHealthStatus() {
        return healthStatus;
    }

    public void setHealthStatus(TaskTrackerHealthStatus healthStatus) {
        this.healthStatus = healthStatus;
    }

    private boolean isTaskRunning(TaskStatus taskStatus) {
        return taskStatus.getState() == TaskStatus.RUNNING;
    }

    public int countRunningMapTask() {
        int count = 0;
        for (TaskStatus status : taskReports) {
            if (status.isMapTask() && isTaskRunning(status)) {
                count++;
            }
        }
        return count;
    }

    public int countRunningReduceTask() {
        int count = 0;
        for (TaskStatus status : taskReports) {
            if (!status.isMapTask() && isTaskRunning(status)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Static class which encapsulates the Node health
     * related fields.
     */
    static class TaskTrackerHealthStatus implements Serializable {

        private boolean isNodeHealthy;
        private String healthReport;
        private long lastReported;

        public TaskTrackerHealthStatus(boolean isNodeHealthy, String healthReport,
                                       long lastReported) {
            this.isNodeHealthy = isNodeHealthy;
            this.healthReport = healthReport;
            this.lastReported = lastReported;
        }

        public TaskTrackerHealthStatus() {
            this.isNodeHealthy = true;
            this.healthReport = "";
            this.lastReported = System.currentTimeMillis();
        }

        /**
         * Sets whether or not a task tracker is healthy or not, based on the
         * output from the node health script.
         *
         * @param isNodeHealthy
         */
        void setNodeHealthy(boolean isNodeHealthy) {
            this.isNodeHealthy = isNodeHealthy;
        }

        /**
         * Returns if node is healthy or not based on result from node health
         * script.
         *
         * @return true if the node is healthy.
         */
        boolean isNodeHealthy() {
            return isNodeHealthy;
        }

        /**
         * Sets the health report based on the output from the health script.
         *
         * @param healthReport
         *          String listing cause of failure.
         */
        void setHealthReport(String healthReport) {
            this.healthReport = healthReport;
        }

        /**
         * Returns the health report of the node if any, The health report is
         * only populated when the node is not healthy.
         *
         * @return health report of the node if any
         */
        String getHealthReport() {
            return healthReport;
        }

        /**
         * Sets when the TT got its health information last
         * from node health monitoring service.
         *
         * @param lastReported last reported time by node
         * health script
         */
        public void setLastReported(long lastReported) {
            this.lastReported = lastReported;
        }

        /**
         * Gets time of most recent node health update.
         *
         * @return time stamp of most recent health update.
         */
        public long getLastReported() {
            return lastReported;
        }
    }
}
