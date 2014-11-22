all:
	find -name "*.java" | xargs javac -d bin/

master:
	java -cp bin/ lxu.lxdfs.namenode.NameNode

slave.datanode:
	java -cp bin/ lxu.lxdfs.datanode.DataNode

slave.tasktracker:
	java -cp bin/ lxu.lxmapreduce.task.TaskTracker

dfsclient:
	java -cp bin/ lxu.lxdfs.client.Client

job:
	java -cp bin/ TestJob
