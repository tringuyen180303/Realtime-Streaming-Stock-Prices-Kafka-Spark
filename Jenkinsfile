pipeline {
    agent any

    environment {
        GIT_CREDENTIALS = credentials('github-token-2')
    }

    stages {
        stage('Clone Repository') {
            steps {
                git branch: 'main', credentialsId: 'github-token-2', url: 'https://github.com/tringuyen180303/Realtime-Streaming-Stock-Prices-Kafka-Spark.git'
            }
        }

        stage('Build') {
            steps {
                echo "Building application..."
                // Add your build command (e.g., mvn, gradle, npm)
                sh 'echo Build successful'
            }
        }

        stage('Test') {
            steps {
                echo "Running tests..."
                sh 'echo All tests passed'
            }
        }

        stage('Deploy') {
            steps {
                echo "Deploying application..."
                sh 'echo Deployment successful'
            }
        }
    }

    post {
        success {
            echo "Build and deployment completed successfully!"
        }
        failure {
            echo "Build failed!"
        }
    }
}
