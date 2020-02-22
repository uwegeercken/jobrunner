#!/bin/bash

# run the CoordinationServer who will
# coordinate the execution of etl's and 
# reports

currentdir=$(dirname $0)

jobrunner_library=jobrunner-1.0.jar
json_library=json-simple-1.1.jar


java -cp ${currentdir}/${jobrunner_library}:${currentdir}/${json_library} com.datamelt.coordination.CoordinationServer "${currentdir}/server.properties"
