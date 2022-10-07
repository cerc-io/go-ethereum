pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script{
                    docker.withRegistry('https://git.vdb.to'){
                        echo 'Building geth image...'
                        def geth_image = docker.build("cerc-io/go-ethereum:jenkinscicd")
                        echo 'built geth image'
                    }
                }
            }
        }
        stage('Test') {
            agent {
                docker {
                    image 'cerc-io/foundation:jenkinscicd'
                    //image 'cerc-io/go-ethereum:jenkinscicd'
                }
            }
            environment {
                GO111MODULE = 'on'
                CGO_ENABLED = 1
                //GOPATH = "${JENKINS_HOME}/jobs/${JOB_NAME}/builds/${BUILD_ID}"
                GOPATH = "/tmp/go"
                //GOMODCACHE = "${WORKSPACE}/pkg/mod"
                //GOCACHE = "${WORKSPACE}/.cache/go-build"
                //GOENV = "${WORKSPACE}/.config/go/env"
                GOMODCACHE = "/tmp/go/pkg/mod"
                GOCACHE = "/tmp/go/.cache/go-build"
                GOENV = "/tmp/go/.config/go/env"
                GOMOD="/dev/null"
                GOFLAGS="-mod=mod"

            }
            steps {
                echo 'Testing ...'
                sh 'env'
                sh 'go env'
                sh 'whoami'
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