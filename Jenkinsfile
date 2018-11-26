BUILD_FOLDER = '/go'
github_user = "gkirok"
docker_user = "gallziguazio"

def build_nuclio(TAG_VERSION) {
    withCredentials([
            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
    ]) {
        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/tsdb-nuclio.git src/github.com/v3io/tsdb-nuclio
                    cd ${BUILD_FOLDER}/src/github.com/v3io/tsdb-nuclio
                    rm -rf functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/v3io-tsdb.git functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    cd functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    rm -rf .git vendor/github.com/v3io vendor/github.com/nuclio
                    cd ${BUILD_FOLDER}/src/github.com/v3io/tsdb-nuclio
                    cp -R functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                """
            }
        }
//                                git checkout ${V3IO_TSDB_VERSION}

//        stage('build in dood') {
//            container('docker-cmd') {
//                sh """
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/tsdb-nuclio/functions/ingest
//                    docker build . --tag tsdb-ingest:latest --tag ${docker_user}/tsdb-ingest:${TAG_VERSION}
//
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/tsdb-nuclio/functions/query
//                    docker build . --tag tsdb-query:latest --tag ${docker_user}/tsdb-query:${TAG_VERSION}
//                """
//                withDockerRegistry([credentialsId: "472293cc-61bc-4e9f-aecb-1d8a73827fae", url: ""]) {
//                    sh "docker push ${docker_user}/tsdb-ingest:${TAG_VERSION}"
//                    sh "docker push ${docker_user}/tsdb-query:${TAG_VERSION}"
//                }
//            }
//        }

        stage('git push') {
            container('jnlp') {
                try {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        cd ${BUILD_FOLDER}/src/github.com/v3io/tsdb-nuclio
                        git add *
                        git commit -am 'Updated TSDB to latest';
                        git push origin master
                    """
                } catch (err) {
                    echo "Can not push code to git"
                }
            }
        }
    }
}


def label = "${UUID.randomUUID().toString()}"
properties([pipelineTriggers([[$class: 'PeriodicFolderTrigger', interval: '2m']])])
podTemplate(label: "v3io-tsdb", yaml: """
apiVersion: v1
kind: Pod
metadata:
  name: "v3io-tsdb"
  labels:
    jenkins/kube-default: "true"
    app: "jenkins"
    component: "agent"
spec:
  shareProcessNamespace: true
  replicas: 3
  containers:
    - name: jnlp
      image: jenkinsci/jnlp-slave
      resources:
        limits:
          cpu: 1
          memory: 2Gi
        requests:
          cpu: 1
          memory: 2Gi
      volumeMounts:
        - name: go-shared
          mountPath: /go
    - name: docker-cmd
      image: docker
      command: [ "/bin/sh", "-c", "--" ]
      args: [ "while true; do sleep 30; done;" ]
      volumeMounts:
        - name: docker-sock
          mountPath: /var/run
        - name: go-shared
          mountPath: /go
  volumes:
    - name: docker-sock
      hostPath:
          path: /var/run
    - name: go-shared
      emptyDir: {}
"""
) {
    parallel(
        'tsdb-nuclio': {
            podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: 'v3io-tsdb') {
                node("v3io-tsdb-nuclio-${label}") {
                    withCredentials([
//                        usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
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
                                        curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"gkirok\\", name: \\"tsdb-nuclio\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > ~/last_tag;
                                        cat ~/last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > ~/tmp_tag
                                        cat ~/tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > ~/next_version
                                    """
                                }

                                def NEXT_VERSION = sh(
                                        script: "cat ~/next_version",
                                        returnStdout: true
                                ).trim()

                                echo "$NEXT_VERSION"

//                            stage ('starting tsdb-nuclio job') {
//                                build job: "tsdb-nuclio/v0.0.8", propagate: true, wait: true, parameters: [[$class: 'StringParameterValue', name: 'TAG_NAME', value: "v${NEXT_VERSION}"]]
//                            }

                                build_nuclio(NEXT_VERSION)

                                stage('create tsdb-nuclio release') {
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/tsdb-nuclio/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
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
//                        usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
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
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/iguazio_api_examples/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
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
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/prometheus/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
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
}