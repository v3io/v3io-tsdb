label = "${UUID.randomUUID().toString()}"
BUILD_FOLDER = "/go"
git_project = "v3io-tsdb"
git_project_user = "gkirok"
git_deploy_user = "iguazio-dev-git-user"
git_deploy_user_token = "iguazio-dev-git-user-token"

def build_nuclio() {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'tsdb-nuclio'
        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/${git_project}.git src/github.com/v3io/${git_project}
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
                    rm -rf functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/v3io-tsdb.git functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    cd functions/ingest/vendor/github.com/v3io/v3io-tsdb
                    rm -rf .git vendor/github.com/v3io vendor/github.com/nuclio
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
                    cp -R functions/ingest/vendor/github.com/v3io/v3io-tsdb functions/query/vendor/github.com/v3io/v3io-tsdb
                """

//                    git checkout ${V3IO_TSDB_VERSION}
            }
        }

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

def build_demo() {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'iguazio_api_examples'

        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/${git_project}.git src/github.com/v3io/${git_project}
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops_demo/golang/src/github.com/v3io/demos
                    rm -rf vendor/github.com/v3io/v3io-tsdb/
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/v3io-tsdb.git vendor/github.com/v3io/v3io-tsdb
                    cd vendor/github.com/v3io/v3io-tsdb
                    rm -rf .git vendor/github.com/v3io vendor/github.com/nuclio
                """

//                    git checkout ${V3IO_TSDB_VERSION}
            }
        }

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
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
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
                        git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/${git_project}.git src/github.com/${git_project}/${git_project}
                    fi
                    cd ${BUILD_FOLDER}/src/github.com/${git_project}/${git_project}
                    rm -rf vendor/github.com/v3io/v3io-tsdb/
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/v3io-tsdb.git vendor/github.com/v3io/v3io-tsdb
                    cd vendor/github.com/v3io/v3io-tsdb
                    git checkout "v${V3IO_TSDB_VERSION}"
                    rm -rf .git vendor/github.com/${git_project}
                """
            }
        }

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

properties([pipelineTriggers([[$class: 'PeriodicFolderTrigger', interval: '2m']])])
podTemplate(label: "${git_project}-${label}", yaml: """
apiVersion: v1
kind: Pod
metadata:
  name: "${git_project}-${label}"
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
    def MAIN_TAG_VERSION
    def PUBLISHED_BEFORE

    node("${git_project}-${label}") {
        withCredentials([
                usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
        ]) {
            stage('get tag data') {
                container('jnlp') {
                    MAIN_TAG_VERSION = sh(
                            script: "echo ${TAG_NAME} | tr -d '\\n' | egrep '^v[\\.0-9]*.*\$' | sed 's/v//'",
                            returnStdout: true
                    ).trim()

                    sh "curl -v -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/gkirok/${git_project}/releases/tags/v${MAIN_TAG_VERSION} > ~/tag_version"

                    PUBLISHED_BEFORE = sh(
                            script: "tag_published_at=\$(cat ~/tag_version | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"published_at\"]'); SECONDS=\$(expr \$(date +%s) - \$(date -d \"\$tag_published_at\" +%s)); expr \$SECONDS / 60 + 1",
                            returnStdout: true
                    ).trim()

                    echo "$MAIN_TAG_VERSION"
                    echo "$PUBLISHED_BEFORE"
                }
            }
        }
    }

    if ( MAIN_TAG_VERSION && PUBLISHED_BEFORE < 240 ) {
        parallel(
            'tsdb-nuclio': {
                podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: "${git_project}-${label}") {
                    node("v3io-tsdb-nuclio-${label}") {
                        withCredentials([
//                            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
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

                                build_nuclio()

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
                podTemplate(label: "v3io-tsdb-netops-demo-${label}", inheritFrom: "${git_project}-${label}") {
                    node("v3io-tsdb-netops-demo-${label}") {
                        withCredentials([
//                            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
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

                                build_demo()

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
                podTemplate(label: "v3io-tsdb-prometheus-${label}", inheritFrom: "${git_project}-${label}") {
                    node("v3io-tsdb-prometheus-${label}") {
                        withCredentials([
                                usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                        ]) {
                            def TAG_VERSION
                            def NEXT_VERSION

                            stage('trigger') {
                                container('jnlp') {
                                    sh """
                                        cd ${BUILD_FOLDER}
                                        git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/prometheus.git src/github.com/prometheus/prometheus
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
    } else {
        stage('warning') {
            if (PUBLISHED_BEFORE >= 240) {
                echo "Tag too old, published before $PUBLISHED_BEFORE minutes."
            } else {
                echo "${TAG_VERSION} is not release tag."
            }
        }
    }
}