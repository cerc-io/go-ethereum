pipeline {
    agent any

    stages {
        stage('Build') {
            agent {
                docker {
                    image 'ubuntu:latest'
                    reuseNode true
                }
            }
            steps {
                docker.withRegistry('https://git.vdb.to'){
                    echo 'Building ...'
                    def geth_image = docker.build("cerc-io/go-ethereum")
                    echo 'built geth'
                }
            }
        }
        stage('Test') {
            steps {
                echo 'Testing ...'
            }
        }
        stage('Packaging') {
            steps {
                echo 'Packaging ...'
            }
        }
    }
}