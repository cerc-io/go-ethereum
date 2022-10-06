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
                GOPATH = "${JENKINS_HOME}/jobs/${JOB_NAME}/builds/${BUILD_ID}"
                GOMODCACHE = "${WORKSPACE}/pkg/mod"
                GOCACHE = "${WORKSPACE}/.cache/go-build"
                GOENV = "${WORKSPACE}/.config/go/env"
            }
            steps {
                echo 'Testing ...'
                sh 'env'
                sh 'go mod tidy'
                sh 'go get -d ./...'
                sh 'go env'
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