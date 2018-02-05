/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

#!groovy

library identifier: 'pipeline-helper@feature/trunk-based-releases',
        retriever: modernSCM(
                [$class       : 'GitSCMSource',
                 credentialsId: '',
                 id           : '8ae53627-89db-4535-92fd-218ac8e6435f',
                 remote       : 'https://code.sbb.ch/scm/kd_esta/pipeline-helper.git'])



pipeline {
    agent { label 'java' }
    options {
        disableConcurrentBuilds()
    }
    tools {
        maven 'Apache Maven 3.3'
        jdk 'OpenJDK 1.8 64-Bit'
    }
    stages {
        stage('release master') {
            when {
                branch 'master'
            }
            steps {
                ci_releaseMvnLean([:])
            }
        }
    }
    post {
        failure {
            // send mail whenever a build fails:
            emailext(
                    subject: "FAILED: ${env.JOB_NAME} [${env.BUILD_NUMBER}]",
                    body: """FAILED: Jenkins continuous/nightly build '${env.JOB_NAME} [${
                        env.BUILD_NUMBER
                    }]':

Check console output at ${env.BUILD_URL}console""",
                    recipientProviders: [[$class: 'DevelopersRecipientProvider']]
            )
        }
    }
}