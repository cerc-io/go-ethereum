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
                echo 'Building ...'
                docker build -t cerc-io/go-ethereum .
                echo 'built geth'
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