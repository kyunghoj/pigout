#!/bin/bash  -x

# TODO: Generate once, run multiple times

## Set variables according to your environment. 
#export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export OOZIE_HOST=dumbo # Where Oozie workflow scheduler is running
#export OOZIE_HOST=ec2-54-202-159-53.us-west-2.compute.amazonaws.com # Where Oozie workflow scheduler is running

#PIGSCRIPT=../experiment/PigOutMix/L7/L7.pig
#PIGSCRIPT=../experiment/PigOutMix/L4/L4.pig
#PIGSCRIPT=../experiment/PigOutMix/L5/L5.pig
#PIGSCRIPT=../experiment/PigOutMix/L11/L11.pig
#PIGSCRIPT=../experiment/PigOutMix/L6/L6.pig
PIGSCRIPT=../experiment/PigOutMix/$1/$1.pig
#PIGSCRIPT=../experiment/PigOutMix/$1/$1_opt.pig

export OOZIE_URL=http://${OOZIE_HOST}:11000/oozie
PIGSCRIPT_BASENAME=`basename $PIGSCRIPT`
OOZIE_JOB_NAME="PigOut_${PIGSCRIPT_BASENAME}"
OOZIE_DIR="./oozie/new/${OOZIE_JOB_NAME}"

# remove previously generated files in local
#rm -rf ${OOZIE_DIR}_org
#mv  ${OOZIE_DIR} ${OOZIE_DIR}_org

# Run pigout to generate
#bin/pigout -f ${PIGSCRIPT}

# Clear things from the previous run

listing=`ls ${OOZIE_DIR}`

for PART in ${listing}
do
    echo "PART=${PART}"

    NAMENODEIN=$(cat ${OOZIE_DIR}/${PART}/coordinator.properties | grep nameNodeIn | cut -d "=" -f 2 | sed 's/\\//g')
    NN_HOSTNAME=$(echo $NAMENODEIN | cut -d ":" -f 2 | cut -d "/" -f 3)
    WF_APP_PATH=${NAMENODEIN}/user/pigout/workflows/${OOZIE_JOB_NAME}
    WF_APP_TMP_PATH=${NAMENODEIN}/user/pigout/workflows/${OOZIE_JOB_NAME}/tmp
    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	    "/usr/local/hadoop/bin/hadoop dfs -rmr ${WF_APP_PATH}/${PART}"

    # copy generated files to hdfs
    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    	"/usr/local/hadoop/bin/hadoop dfs -mkdir ${WF_APP_PATH}/${PART}"

    REMOTE_HOME=`ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "pwd"`

    cp oozie-templates/remote-copy ${OOZIE_DIR}/${PART}/

    mkdir ${OOZIE_DIR}/${PART}/lib
    cp ../experiment/PigOutMix/pigmix.jar ${OOZIE_DIR}/${PART}/lib

    rsync -avz ${OOZIE_DIR}/${PART} ${USER}@${NN_HOSTNAME}:${REMOTE_HOME}/tmp

    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	    "/usr/local/hadoop/bin/hadoop dfs -put ${REMOTE_HOME}/tmp/${PART}/*  ${WF_APP_PATH}/${PART}"

    # Mkdir workflow_app_path/lib
    echo "Mkdir ${WF_APP_PATH}/${PART}/lib ..."
    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    	"/usr/local/hadoop/bin/hadoop dfs -mkdir ${WF_APP_PATH}/${PART}/lib"

    # Mkdir workflow_app_path/tmp
    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    	"/usr/local/hadoop/bin/hadoop dfs -rmr ${WF_APP_TMP_PATH}/${PART}"
    ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    	"/usr/local/hadoop/bin/hadoop dfs -mkdir ${WF_APP_TMP_PATH}/${PART}"

    # flag file for initial inputs
    INPUTS=$(cat ${OOZIE_DIR}/${PART}/coordinator.properties \
        | grep "input[1-9]" \
        | grep "hdfs" \
        | cut -d "=" -f 2 \
        | grep -v tmp \
        | sed 's/\\//g')
    for INPUT in ${INPUTS}
    do
        echo "Touching ${INPUT}..."
        ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    		"/usr/local/hadoop/bin/hadoop dfs -touchz ${INPUT}/_SUCCESS"
    done

done

for PART in ${listing}
do
	echo "PART=${PART}"

	NAMENODEIN=$(cat ${OOZIE_DIR}/${PART}/coordinator.properties | grep nameNodeIn | cut -d "=" -f 2 | sed 's/\\//g')
	NN_HOSTNAME=$(echo $NAMENODEIN | cut -d ":" -f 2 | cut -d "/" -f 3)
	WF_APP_PATH=${NAMENODEIN}/user/pigout/workflows/${OOZIE_JOB_NAME}
	WF_APP_TMP_PATH=${NAMENODEIN}/user/pigout/workflows/${OOZIE_JOB_NAME}/tmp

	OUTPUTS=$(cat ${OOZIE_DIR}/${PART}/coordinator.properties \
				| grep "output[1-9]" \
				| cut -d "=" -f 2 \
				| sed 's/\\//g')

	for OUTPUT in ${OUTPUTS}
	do
		echo "Delete ${OUTPUT} from the previous run..."
		ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
				"/usr/local/hadoop/bin/hadoop dfs -rmr ${OUTPUT}/"
	done

	INPUTS=$(cat ${OOZIE_DIR}/${PART}/coordinator.properties \
        | grep "input[1-9]" \
        | grep "hdfs" \
        | cut -d "=" -f 2 \
        | grep tmp \
        | sed 's/\\//g')
	    for INPUT in ${INPUTS}
	    do
		echo "Delete ${INPUT}..."
		ssh ${USER}@${NN_HOSTNAME} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
			"/usr/local/hadoop/bin/hadoop dfs -rmr ${INPUT}"
	    done


	# submit to Oozie!
	export OOZIE_URL=http://${NN_HOSTNAME}:11000/oozie
	oozie job -run -config ${OOZIE_DIR}/${PART}/coordinator.properties
done

exit
