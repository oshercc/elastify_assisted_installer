pipeline {
    agent { label 'download_all_logs' }

    triggers { cron('H/30 * * * *') }

    environment {
        ELASTIC_HOST = credentials('elastic_host')
        LOGS_LOCATION = "../collect_all_prodoction_logs/build/"
        SLACK_TOKEN = credentials('slack-token')

    }
    options {
      timeout(time: 30, unit: 'MINUTES')
    }

    stages {
        stage('Log files') {
            steps{
               sh "python3 log_files_to_elastic.py -elastic-server ${ELASTIC_HOST} --data-path ${LOGS_LOCATION}"
            }
        }
    }

    post {
        failure {
            script {
                def data = [text: "Attention! ${BUILD_TAG} job failed, see: ${BUILD_URL}"]
                writeJSON(file: 'data.txt', json: data, pretty: 4)
            }
            sh '''curl -X POST -H 'Content-type: application/json' --data-binary "@data.txt"  https://hooks.slack.com/services/${SLACK_TOKEN}'''
        }
    }
}
