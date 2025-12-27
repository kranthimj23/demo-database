def image_repo = ''
def image_tag = ''

pipeline {
    agent any

    environment {
        PROJECT_ID = 'mobile-app-1-482109'
        CLUSTER = 'autopilot-cluster-1'
        ZONE = 'us-central1'
        GCP_KEY = '/var/lib/jenkins/keys/mobile-app-1-482109.json'
        PYTHON_EXEC = 'python3.11'
        GIT_CREDENTIALS_ID = credentials('jenkins-token')
    }

    stages {
        stage('Checkout demo-database repo') {
            steps {
                deleteDir()
                script {
                    withCredentials([string(credentialsId: 'jenkins-token', variable: 'GIT_TOKEN')]) {
                        checkout([
                            $class: 'GitSCM',
                            branches: [[name: "*/dev"]],
                            userRemoteConfigs: [[
                                url: "https://${GIT_TOKEN}@github.com/kranthimj23/demo-database.git"
                            ]]
                        ])
                    }
                }
            }
        }

        stage('Checkout zdt-manager-src repo') {
            steps {
                dir('zdt-manager-src') {
                    script {
                        withCredentials([string(credentialsId: 'jenkins-token', variable: 'GIT_TOKEN')]) {
                            checkout([
                                $class: 'GitSCM',
                                branches: [[name: "*/zdt-application"]],
                                userRemoteConfigs: [[
                                    url: "https://${GIT_TOKEN}@github.com/kranthimj23/zdt-manager-src.git"
                                ]]
                            ])
                        }
                    }
                }
            }
        }

        stage('Authenticate with GCP') {
            steps {
                sh """
                    gcloud auth activate-service-account --key-file="${GCP_KEY}"
                    gcloud config set project ${PROJECT_ID}
                    gcloud auth configure-docker us-central1-docker.pkg.dev --quiet
                    gcloud auth list
                """
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    image_repo = "us-central1-docker.pkg.dev/${env.PROJECT_ID}/demo-database/database"
                    image_tag = "${BUILD_NUMBER}-${env.env_namespace ?: 'dev'}"
                    def image_full = "${image_repo}:${image_tag}"

                    sh """
                        echo 'Logging in to Artifact Registry...'
                        gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev

                        echo 'Building Docker image...'
                        docker build -t ${image_full} .

                        echo 'Pushing Docker image...'
                        docker push ${image_full}
                    """
                }
            }
        }

        stage('Deploy to GKE') {
            steps {
                sh """
                    gcloud container clusters get-credentials ${CLUSTER} --zone ${ZONE} --project ${PROJECT_ID}
                """
                script {
                    withCredentials([string(credentialsId: 'jenkins-token', variable: 'GIT_TOKEN')]) {
                        def deployScriptPath = "zdt-manager-src/backend/app/ci/scripts/deploy_to_gke.py"
        
                        def pythonCommand = """
                            export CLUSTER=${CLUSTER}
                            export ZONE=${ZONE}
                            export PROJECT_ID=${PROJECT_ID}
                            export GIT_TOKEN=${GIT_TOKEN}
                            echo Running Python script...
                            ${PYTHON_EXEC} ${deployScriptPath} ${env.env_namespace ?: 'dev'} ${image_repo} ${image_tag} ${env.github_url} ${env.microservice}
                        """
        
                        echo "Executing Python Deployment Script..."
                        def result = sh(script: pythonCommand, returnStdout: true).trim()
                        echo "Deployment Output:\n${result}"
                    }
                }
            }
        }
    }
}
