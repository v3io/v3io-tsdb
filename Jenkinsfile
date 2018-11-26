def BUILD_FOLDER = '/go'
def github_user = "gkirok"
def docker_user = "gallziguazio"

def label = "${UUID.randomUUID().toString()}"
properties([pipelineTriggers([[$class: 'PeriodicFolderTrigger', interval: '2m']])])
parallel(
    'tsdb-nuclio': {
        podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: 'kube-slave-dood') {
            node("v3io-tsdb-nuclio-${label}") {
                withCredentials([
                        usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                        string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                ]) {
                    stage('trigger') {
                        def TAG_VERSION = sh(
                                script: "echo ${TAG_NAME} | tr -d '\\n' | egrep '^v[\\.0-9]*\$'",
                                returnStdout: true
                        ).trim()

                        if (TAG_VERSION) {
                            stage('get previous release version') {
                                sh """
                                    curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"gkirok\\", name: \\"tsdb-nuclio\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > last_tag;
                                    cat last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > tmp_tag
                                    cat tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > next_version
                                """
                            }

                            def NEXT_VERSION = sh(
                                    script: "cat next_version",
                                    returnStdout: true
                            ).trim()

                            echo "$NEXT_VERSION"

                            stage('create tsdb-nuclio release') {
                                sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/tsdb-nuclio/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Auto release, triggered by v3io-tsdb\"}'"
                            }
                        }
                    }
                }
            }
        }
    },
    'netops-demo': {
        podTemplate(label: "v3io-tsdb-netops-demo-${label}", inheritFrom: 'kube-slave-dood') {
            node("v3io-tsdb-netops-demo-${label}") {
                withCredentials([
                        usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                        string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                ]) {
                    stage('trigger') {
                        def TAG_VERSION = sh(
                                script: "echo ${TAG_NAME} | tr -d '\\n' | egrep '^v[\\.0-9]*\$'",
                                returnStdout: true
                        ).trim()

                        if (TAG_VERSION) {
                            stage('get previous release version') {
                                sh """
                                    curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"gkirok\\", name: \\"iguazio_api_examples\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > last_tag;
                                    cat last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > tmp_tag
                                    cat tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > next_version
                                """
                            }

                            def NEXT_VERSION = sh(
                                    script: "cat next_version",
                                    returnStdout: true
                            ).trim()

                            echo "$NEXT_VERSION"

                            stage('create iguazio_api_examples release') {
                                sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/iguazio_api_examples/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Auto release, triggered by v3io-tsdb\"}'"
                            }
                        }
                    }
                }
            }
        }
    },
    'prometheus': {
        podTemplate(label: "v3io-tsdb-prometheus-${label}", inheritFrom: 'kube-slave-dood') {
            node("v3io-tsdb-prometheus-${label}") {
                withCredentials([
                        usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                        string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                ]) {
                    stage('trigger') {
                        stage('prepare sources') {
                            sh """
                                cd ${BUILD_FOLDER}
                                git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/prometheus.git src/github.com/prometheus/prometheus
                            """
                        }

                        def TAG_VERSION = sh(
                                script: "cat ${BUILD_FOLDER}/src/github.com/prometheus/prometheus/VERSION",
                                returnStdout: true
                        ).trim()

                        if (TAG_VERSION) {
                            def NEXT_VERSION = "${TAG_VERSION}-${TAG_NAME}"

                            echo "$NEXT_VERSION"

                            stage('create prometheus release') {
                                sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/prometheus/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Auto release, triggered by v3io-tsdb\"}'"
                            }

//                            def PROMETHEUS_VERSION = sh(
//                                    script: "cat ${BUILD_FOLDER}/src/github.com/v3io-tsdb/v3io-tsdb/VERSION",
//                                    returnStdout: true
//                            ).trim()
                        }
                    }
                }
            }
        }
    }
)