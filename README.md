# jobrunner
Program to run Pentaho Data Integration ETL jobs and reports. Allows to chain ETL's so that a job only runs, if the dependant job has finished.

Settings for the server start are defined in the server.properties file. The ETL job definitions are defined in the jobs.json file.

There are two PDI jobs (.kjb) and a transformation (.ktr). The jobs generate some data and output the data to a file.

To get started:

1. download/clone this repository
2. adjust the server.properties file according to your needs
3. adjust the jobs.json file according to your needs
4. run the runserver.sh script
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

