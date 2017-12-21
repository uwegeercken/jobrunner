#!/bin/bash

# send a client message to the CoordinationServer

library_root=/home/uwe/development/jars

jobrunner_library=jobrunner-1.0.jar
json_library=json-simple-1.1.jar


java -cp ./:${jobrunner_library}:${library_root}/${json_library} com.datamelt.coordination.CoordinationClientMessage -m=${1}
