#!/bin/bash

# send a client message to the CoordinationServer

currentdir=$(dirname $0)

jobrunner_library=jobrunner-1.0.jar

java -cp ${currentdir}/${jobrunner_library} com.datamelt.coordination.CoordinationClientMessage -m="${1}"
