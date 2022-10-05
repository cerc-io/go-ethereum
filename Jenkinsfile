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
                    image 'ubuntu:latest'
                }
            }
            tools {
                    go 'go-1.18.5'
                }
                environment {
                    GO111MODULE = 'on'
                    CGO_ENABLED = 0
                    GOPATH = "${JENKINS_HOME}/jobs/${JOB_NAME}/builds/${BUILD_ID}"
                }
            steps {
                echo 'Testing ...'
                sh 'sudo apt-get install -y make'
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