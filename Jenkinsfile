pipeline {
    agent any

    options{
        // Max number of build logs to keep and days to keep
        buildDiscarder(logRotator(numToKeepStr: '5', daysToKeepStr: '5'))
        // Enable timestamp at each job in the pipeline
        timestamps()
    }

    environment{
        registry = 'dongtd6/text-sentiment-classifier' //Image id push to Docker Hub
        registryCredential = 'dockerhub'    //Credential on Jenkins to login Docker Hub  
    }

    stages {
        stage('Test') { //Use test_model_correctness.py
            agent {
                docker {
                    image 'python:3.8' // Use a Docker image with Python 3.8
                }
            }
            steps {
                echo 'Installing test dependencies...'
                sh 'uv pip install -r requirements.txt'

            }
            // steps {
            //     echo 'Testing model correctness..'
            //     sh 'pytest'
            //     //sh 'pytest tests/test_model_correctness.py' // Run the test script  
            // }
            steps {
                echo 'Testing model correctness and checking coverage...'
                script {
                    try {
                        // Chạy pytest với coverage và lưu kết quả vào biến
                        // --cov=. để đo coverage cho toàn bộ project (thư mục hiện tại)
                        // --cov-report=term-missing để hiển thị chi tiết các dòng thiếu test
                        // --cov-report=xml để tạo báo cáo XML (có thể dùng cho các công cụ khác như SonarQube)
                        // || true để đảm bảo lệnh không fail ngay cả khi test fail, chúng ta sẽ kiểm tra coverage sau
                        def testOutput = sh(script: 'pytest --cov=. --cov-report=term-missing', returnStdout: true).trim()

                        echo testOutput

                        // Phân tích output để lấy phần trăm coverage
                        def coverageMatch = testOutput =~ /TOTAL\s+\d+\s+\d+\s+(\d+)%/
                        if (coverageMatch) {
                            def coveragePercentage = coverageMatch[0][1].toInteger()
                            echo "Code Coverage: ${coveragePercentage}%"

                            if (coveragePercentage > 80) {
                                echo "Coverage ${coveragePercentage}% is greater than 80%. Proceeding to next steps."
                            } else {
                                error "Coverage ${coveragePercentage}% is NOT greater than 80%. Failing build."
                            }
                        } else {
                            error "Could not parse coverage percentage from pytest output."
                        }
                    } catch (e) {
                        // Xử lý lỗi nếu pytest fail hoặc có vấn đề khác
                        error "Tests failed or an error occurred during testing: ${e.message}"
                    }
                }
            }
        }
        stage('Build') {
            steps {
                script {
                    echo 'Building image for deployment..'
                    dockerImage = docker.build registry + ":$BUILD_NUMBER" //Build images from Dockerfile
                    echo 'Pushing image to dockerhub..'
                    docker.withRegistry( '', registryCredential ) {  //Push to Docker Hub
                        dockerImage.push()
                        dockerImage.push('latest')
                    }
                }
            }
        }
        stage('Deploy') {
            agent {
                kubernetes {
                    containerTemplate {
                        name 'helm-container' // Name of the container to be used for helm upgrade
                        image 'quandvrobusto/jenkins:lts-jdk17' //  alpine/helm:3.14.0 The image containing helm
                        imagePullPolicy 'Always' // Always pull image in case of using the same tag
                    }
                }
            }
            steps {
                script {
                    echo 'Deploying models..'
                    container('helm-container') {
                        sh("helm upgrade --install tsc ./helm-charts/model-deployment/ --namespace model-serving")
                    }
                }
            }
        }
    }
}