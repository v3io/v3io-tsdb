BUILD_FOLDER = '/go'
github_user = "gkirok"
docker_user = "gallziguazio"

def build_nuclio(TAG_VERSION) {
    withCredentials([
            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'tsdb-nuclio'
        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/${git_project}.git src/github.com/v3io/${git_project}
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
                    rm -rf functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/v3io-tsdb.git functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    cd functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    rm -rf .git vendor/github.com/v3io vendor/github.com/nuclio
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
                    cp -R functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                """

//                    git checkout ${V3IO_TSDB_VERSION}
            }
        }

//        stage('build in dood') {
//            container('docker-cmd') {
//                sh """
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/functions/ingest
//                    docker build . --tag tsdb-ingest:latest --tag ${docker_user}/tsdb-ingest:${TAG_VERSION}
//
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/functions/query
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
                        cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
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


def build_demo(TAG_VERSION) {
    withCredentials([
            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'iguazio_api_examples'

        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/${git_project}.git src/github.com/v3io/${git_project}
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops_demo/golang/src/github.com/v3io/demos
                    rm -rf vendor/github.com/v3io/v3io-tsdb/
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/v3io-tsdb.git vendor/github.com/v3io/v3io-tsdb
                    cd vendor/github.com/v3io/v3io-tsdb
                    rm -rf .git vendor/github.com/v3io vendor/github.com/nuclio
                """

//                    git checkout ${V3IO_TSDB_VERSION}
            }
        }

//        stage('build in dood') {
//            container('docker-cmd') {
//                sh """
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops_demo/golang/src/github.com/v3io/demos
//                    docker build . --tag netops-demo-golang:latest --tag ${docker_user}/netops-demo-golang:$NETOPS_DEMO_VERSION --build-arg NUCLIO_BUILD_OFFLINE=true --build-arg NUCLIO_BUILD_IMAGE_HANDLER_DIR=github.com/v3io/demos
//
//                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops_demo/py
//                    docker build . --tag netops-demo-py:latest --tag ${docker_user}/netops-demo-py:$NETOPS_DEMO_VERSION
//                """
//                withDockerRegistry([credentialsId: "472293cc-61bc-4e9f-aecb-1d8a73827fae", url: ""]) {
//                    sh "docker push ${docker_user}/netops-demo-golang:${NETOPS_DEMO_VERSION}"
//                    sh "docker push ${docker_user}/netops-demo-py:${NETOPS_DEMO_VERSION}"
//                }
//            }
//        }

        stage('git push') {
            container('jnlp') {
                try {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops_demo
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

def build_prometheus(TAG_VERSION) {
    withCredentials([
            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'prometheus'
        def V3IO_TSDB_VERSION

        stage('prepare sources') {
            container('jnlp') {
                V3IO_TSDB_VERSION = sh(
                        script: "echo ${TAG_VERSION} | awk -F '-v' '{print \$2}'",
                        returnStdout: true
                ).trim()

                sh """ 
                    cd ${BUILD_FOLDER}
                    if [[ ! -d src/github.com/${git_project}/${git_project} ]]; then 
                        git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/${git_project}.git src/github.com/${git_project}/${git_project}
                    fi
                    cd ${BUILD_FOLDER}/src/github.com/${git_project}/${git_project}
                    rm -rf vendor/github.com/v3io/v3io-tsdb/
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/v3io-tsdb.git vendor/github.com/v3io/v3io-tsdb
                    cd vendor/github.com/v3io/v3io-tsdb
                    git checkout "v${V3IO_TSDB_VERSION}"
                    rm -rf .git vendor/github.com/${git_project}
                """
            }
        }

//        stage('build in dood') {
//            container('docker-cmd') {
//                sh """
//                    cd ${BUILD_FOLDER}/src/github.com/${git_project}/${git_project}
//                    docker build . -t ${docker_user}/v3io-prom:${TAG_VERSION} -f Dockerfile.multi
//                """
//                withDockerRegistry([credentialsId: "472293cc-61bc-4e9f-aecb-1d8a73827fae", url: ""]) {
//                    sh "docker push ${docker_user}/v3io-prom:${TAG_VERSION}"
//                }
//            }
//        }

        stage('git push') {
            container('jnlp') {
                try {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        cd ${BUILD_FOLDER}/src/github.com/${git_project}/${git_project};
                        git add vendor/github.com/v3io/v3io-tsdb;
                        git commit -am 'Updated TSDB to v${V3IO_TSDB_VERSION}';
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
podTemplate(label: "dood", yaml: """
apiVersion: v1
kind: Pod
metadata:
  name: "dood"
  labels:
    jenkins/kube-default: "true"
    app: "jenkins"
    component: "agent"
spec:
  shareProcessNamespace: true
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
            podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: 'dood') {
                node("v3io-tsdb-nuclio-${label}") {
                    withCredentials([
//                            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                    ]) {
                        def TAG_VERSION
                        def NEXT_VERSION

                        stage('trigger') {
                            container('jnlp') {
                                TAG_VERSION = sh(
                                        script: "echo ${TAG_NAME} | tr -d '\\n' | egrep '^v[\\.0-9]*\$'",
                                        returnStdout: true
                                ).trim()
                            }
                        }

                        if (TAG_VERSION) {
                            stage('get previous release version') {
                                container('jnlp') {
                                    sh """
                                        curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"gkirok\\", name: \\"tsdb-nuclio\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > ~/last_tag;
                                        cat ~/last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > ~/tmp_tag
                                        cat ~/tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > ~/next_version
                                    """
                                    NEXT_VERSION = sh(
                                            script: "cat ~/next_version",
                                            returnStdout: true
                                    ).trim()

                                    echo "$NEXT_VERSION"
                                }
                            }

//                            stage ('starting tsdb-nuclio job') {
//                                build job: "tsdb-nuclio/v0.0.8", propagate: true, wait: true, parameters: [[$class: 'StringParameterValue', name: 'TAG_NAME', value: "v${NEXT_VERSION}"]]
//                            }

                            build_nuclio(NEXT_VERSION)

                            stage('create tsdb-nuclio release') {
                                container('jnlp') {
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/tsdb-nuclio/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
                                }
                            }
                        }

                    }
                }
            }
        },
        'netops-demo': {
            podTemplate(label: "v3io-tsdb-netops-demo-${label}", inheritFrom: 'dood') {
                node("v3io-tsdb-netops-demo-${label}") {
                    withCredentials([
//                            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                    ]) {
                        def TAG_VERSION
                        def NEXT_VERSION

                        stage('trigger') {
                            container('jnlp') {
                                TAG_VERSION = sh(
                                        script: "echo ${TAG_NAME} | tr -d '\\n' | egrep '^v[\\.0-9]*\$'",
                                        returnStdout: true
                                ).trim()
                            }
                        }

                        if (TAG_VERSION) {
                            stage('get previous release version') {
                                container('jnlp') {
                                    sh """
                                        curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"gkirok\\", name: \\"iguazio_api_examples\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > last_tag;
                                        cat last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > tmp_tag
                                        cat tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > next_version
                                    """

                                    NEXT_VERSION = sh(
                                            script: "cat next_version",
                                            returnStdout: true
                                    ).trim()

                                    echo "$NEXT_VERSION"
                                }
                            }

                            build_demo(NEXT_VERSION)

                            stage('create iguazio_api_examples release') {
                                container('jnlp') {
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/iguazio_api_examples/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
                                }
                            }
                        }
                    }
                }
            }
        },
        'prometheus': {
            podTemplate(label: "v3io-tsdb-prometheus-${label}", inheritFrom: 'dood') {
                node("v3io-tsdb-prometheus-${label}") {
                    withCredentials([
                            usernamePassword(credentialsId: '4318b7db-a1af-4775-b871-5a35d3e75c21', passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                            string(credentialsId: 'dd7f75c5-f055-4eb3-9365-e7d04e644211', variable: 'GIT_TOKEN')
                    ]) {
                        def TAG_VERSION
                        def NEXT_VERSION

                        stage('trigger') {
                            container('jnlp') {
                                sh """
                                    cd ${BUILD_FOLDER}
                                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${github_user}/prometheus.git src/github.com/prometheus/prometheus
                                """

                                TAG_VERSION = sh(
                                        script: "cat ${BUILD_FOLDER}/src/github.com/prometheus/prometheus/VERSION",
                                        returnStdout: true
                                ).trim()
                            }
                        }

                        if (TAG_VERSION) {
                            stage('get previous release version') {
                                container('jnlp') {
                                    NEXT_VERSION = "${TAG_VERSION}-${TAG_NAME}"

                                    echo "$NEXT_VERSION"
                                }
                            }

                            build_prometheus(NEXT_VERSION)

                            stage('create prometheus release') {
                                container('jnlp') {
                                    sh "curl -v -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/prometheus/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\"}'"
                                }
                            }
                        }
                    }
                }
            }
        }
    )
}