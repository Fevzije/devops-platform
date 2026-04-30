pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Checkout completed'
            }
        }

        stage('Build API') {
            steps {
                sh 'docker build -t devops-api ./api'
            }
        }

        stage('Build Worker') {
            steps {
                sh 'docker build -t devops-worker ./worker'
            }
        }

        stage('Test Compose Config') {
            steps {
                sh 'docker compose config'
            }
        }
    }
}
