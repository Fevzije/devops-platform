pipeline {
    agent any

    stages {
        stage('DEBUG') {
            steps {
                sh 'pwd'
                sh 'ls -la'
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
        }
        }
    }
}
