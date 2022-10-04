pipeline {
    agent {
        docker { image 'ubuntu:latest'}
    }

    stages {
        stage('Build') {
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