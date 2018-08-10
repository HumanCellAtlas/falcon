#!/usr/bin/env bash

function print_line() {
    printf %"$(tput cols)"s |tr " " "="
}

function create_secret () {
    printf "\nCreating secret object for Falcon...\n"

    local CONFIG_NAME=$1
    local CONFIG_FILE=$2
    local CAAS_KEY_FILE=$3
    
    if [ -z $CAAS_KEY_FILE ]; then
        kubectl create secret generic $CONFIG_NAME --from-file=config=$CONFIG_FILE
    else
        kubectl create secret generic $CONFIG_NAME --from-file=config=$CONFIG_FILE --from-file=caas_key=$CAAS_KEY_FILE
    fi
}

function render_deployment_file() {
    printf "\nRendering the deployment YAML file for Falcon...\n"

    local DOCKER_TAG=$1
    local FALCON_CONFIG=$2
    local USE_CAAS=$3
    
    docker run -i --rm \
        -e DOCKER_TAG=${DOCKER_TAG} \
        -e USE_CAAS=${USE_CAAS} \
        -e FALCON_CONFIG=${FALCON_CONFIG} \
        -v ${PWD}:/working broadinstitute/dsde-toolbox:k8s \
        /usr/local/bin/render-ctmpl.sh -k falcon-deployment.yaml.ctmpl
}

function create_deployment() {
    printf "\nCreating deployment for Falcon...\n"

    kubectl apply -f falcon-deployment.yaml --record
}

function main() {
    local DOCKER_TAG=$1
    local CONFIG_FILE=$2
    local USE_CAAS=${3:-""}
    local CAAS_KEY_FILE=${4:-""}
    local FALCON_CONFIG_NAME="falcon-config-$(date '+%Y-%m-%d-%H-%M')"

    set -e 

    print_line
    create_secret ${FALCON_CONFIG_NAME} ${CONFIG_FILE} ${CAAS_KEY_FILE}

    print_line
    render_deployment_file ${DOCKER_TAG} ${FALCON_CONFIG_NAME} ${USE_CAAS}

    print_line
    create_deployment
}


# Main Runner:
error=0
if [[ -z $1 ]]; then
    printf "\n- You must specify a Falcon version tag!\n"
    error=1
fi

if [[ -z $2 ]]; then
    printf "\n- You must provide the path to a valid config file for Falcon!\n"
    error=1
fi

if [[ -z $3 ]]; then
    printf "\n- Missing boolean value USE_CAAS, will assume not using Cromwell-as-a-Service now.\n"
fi

if [[ -z $4 ]]; then
    printf "\n- Missing the CAAS_KEY_FILE, will assume not using Cromwell-as-a-Service now. You might want to provide a key file for using caas.\n"
fi

if [[ $3 == "true" ]]; then
    if [[ -z $4 ]]; then
        printf "\n- Missing the CAAS_KEY_FILE for caas!\n"
        error=1
    fi
fi

if [[ $error -eq 1 ]]; then
    printf "\nUsage: bash deploy.sh DOCKER_TAG CONFIG_FILE USE_CAAS(optional: true/false) CAAS_KEY_FILE(optional)\n"
    exit 1
fi

main $1 $2 ${3} ${4}
