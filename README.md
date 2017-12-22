# jobrunner
Program to run Pentaho Data Integration ETL jobs and reports. Allows to chain ETL's so that a job only runs, if the dependent job has finished. It is a client/server architecture where the server handles the jobs and execution of jobs and the client sends messages (tasks) to the server. All tasks are multi-threaded - they run as a seperate process.

The idea is that the messages that can be sent to the server are triggered by an existing scheduler such as cron. The server only coordinates the execution of the jobs but does not do the scheduling itself. The json file with the job definitions contains a scheduled start time for each job. This is the planned time when the job should run. When the server is triggered from the external scheduler, the job is run when the scheduled start time is at or before the given time on the same day. If the job has another job defined that it depends on, the job will not start until the dependent job has finished.

The resetjobs message will reset (reset start, finished times, exit code, etc) all jobs and will set their execution date to the current date. I still have to work on this feature to come up with a good implementation.

Settings for the server start are defined in the server.properties file. There are also environment variables that can be passed to the ETL process. The ETL job definitions are defined in the jobs.json file. There are two PDI jobs (.kjb) and a transformation (.ktr). The jobs generate some data and output the data to a file.

To get started:

1. download/clone this repository
2. adjust the server.properties file according to your needs
3. adjust the jobs.json file according to your needs
4. run the runserver.sh script - the server will wait for connection on the given port
5. run the sendmessage.sh and pass a message like this:

./sendmessage.sh uptime

possible messages are:
- uptime
- processid
- hello
- jobfinished
- jobcanstart
- jobstartstatus
- jobstarttime
- jobrun
- jobexitcode
- jobruntime
- resetjobs
- reloadjobs

messages that start with "job" need an extra parameter, which is the job id (see json file):

./sendmessage.sh jobcanstart:id_0001

run an etl job by sending

./sendmessage.sh jobrun:id_0001

Once the job runs, watch the server output. When the job is finished it displays the finished time and exit code. Once finished check the log folder to see the output of the etl run.


Note 1: Report handling is not implemented yet.

Note 2: Handling of multiple dependent jobs is not implemented yet.

