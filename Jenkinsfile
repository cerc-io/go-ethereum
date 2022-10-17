pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script{
                    docker.withRegistry('https://git.vdb.to'){
                        echo 'Building geth image...'
                        //def geth_image = docker.build("cerc-io/go-ethereum:jenkinscicd")
                        echo 'built geth image'
                    }
                }
            }
        }
        stage('Test') {
            agent {
                docker {
                    image 'cerc-io/foundation:jenkinscicd'
                    //image 'cerc-io/foundation_alpine:jenkinscicd'
                }
            }

            environment {
                GO111MODULE = "on"
                CGO_ENABLED = 1
                GOPATH = "${JENKINS_HOME}/jobs/${JOB_NAME}/builds/${BUILD_ID}"
                //GOPATH = "/go"
                //GOPATH = "/tmp/go"
                //GOMODCACHE = "/go/pkg/mod"
                GOCACHE = "${WORKSPACE}/.cache/go-build"
                GOENV = "${WORKSPACE}/.config/go/env"
                //GOMODCACHE = "/tmp/go/pkg/mod"
                //GOMOD="/dev/null"
                //GOFLAGS=""

            }
            steps {
                echo 'Testing ...'
                sh 'env'
                sh 'go env'
                //sh '/usr/local/go/bin/go test -p 1 -v ./...'
                sh 'make test'
            }
        }
        stage('Packaging') {
            steps {
                echo 'Packaging ...'
            }
        }
    }
}