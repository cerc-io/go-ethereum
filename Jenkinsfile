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