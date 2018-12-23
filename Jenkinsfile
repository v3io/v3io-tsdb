label = "${UUID.randomUUID().toString()}"
BUILD_FOLDER = "/go"
expired=240
attempts=15
git_project = "v3io-tsdb"
git_project_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"

def build_v3io_tsdb(TAG_VERSION) {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'v3io-tsdb'
        stage('prepare sources') {
            container('jnlp') {
                dir("${BUILD_FOLDER}/src/github.com/v3io/${git_project}") {
                    git(changelog: false, credentialsId: git_deploy_user_private_key, poll: false, url: "git@github.com:${git_project_user}/${git_project}.git")
                    sh("git checkout v${TAG_VERSION}")
                }
            }
        }

        stage("build ${git_project} binaries in dood") {
            container('golang') {
                sh """
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}
                    GOOS=linux GOARCH=amd64 TRAVIS_TAG=v${TAG_VERSION} make bin
                    GOOS=darwin GOARCH=amd64 TRAVIS_TAG=v${TAG_VERSION} make bin
                    GOOS=windows GOARCH=amd64 TRAVIS_TAG=v${TAG_VERSION} make bin
                    ls -la /go/bin
                """
            }
        }

        stage('upload release assets') {
            container('jnlp') {
                RELEASE_ID = github.get_release_id(git_project, git_project_user, "v${TAG_VERSION}", GIT_TOKEN)

                github.upload_asset(git_project, git_project_user, "tsdbctl-v${TAG_VERSION}-linux-amd64", RELEASE_ID, GIT_TOKEN)
                github.upload_asset(git_project, git_project_user, "tsdbctl-v${TAG_VERSION}-darwin-amd64", RELEASE_ID, GIT_TOKEN)
                github.upload_asset(git_project, git_project_user, "tsdbctl-v${TAG_VERSION}-windows-amd64", RELEASE_ID, GIT_TOKEN)
            }
        }
    }
}

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
        def git_project = 'demos'

        stage('prepare sources') {
            container('jnlp') {
                sh """
                    cd ${BUILD_FOLDER}
                    git clone https://${GIT_USERNAME}:${GIT_PASSWORD}@github.com/${git_project_user}/${git_project}.git src/github.com/v3io/${git_project}
                    cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops/golang/src/github.com/v3io/demos
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
                        cd ${BUILD_FOLDER}/src/github.com/v3io/${git_project}/netops
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
      image: jenkins/jnlp-slave
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
    - name: golang
      image: golang:1.11
      command: [ "/bin/sh", "-c", "--" ]
      args: [ "while true; do sleep 30; done;" ]
      volumeMounts:
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
    def next_versions = ['demos':null, 'prometheus':null, 'tsdb-nuclio':null]

    node("${git_project}-${label}") {
        withCredentials([
                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
        ]) {
            pipelinex = library(identifier: 'pipelinex@DEVOPS-204-pipelinex', retriever: modernSCM(
                    [$class: 'GitSCMSource',
                     credentialsId: git_deploy_user_private_key,
                     remote: "git@github.com:iguazio/pipelinex.git"])).com.iguazio.pipelinex
            multi_credentials=[pipelinex.DockerRepo.ARTIFACTORY_IGUAZIO, pipelinex.DockerRepo.DOCKER_HUB, pipelinex.DockerRepo.QUAY_IO]

            stage('get tag data') {
                container('jnlp') {
                    MAIN_TAG_VERSION = github.get_tag_version(TAG_NAME)
                    PUBLISHED_BEFORE = github.get_tag_published_before(git_project, git_project_user, "v${MAIN_TAG_VERSION}", GIT_TOKEN)

                    echo "$MAIN_TAG_VERSION"
                    echo "$PUBLISHED_BEFORE"
                }
            }
        }
    }

    if ( MAIN_TAG_VERSION != null && MAIN_TAG_VERSION.length() > 0 && PUBLISHED_BEFORE < expired ) {
        parallel(
            'v3io-tsdb': {
                podTemplate(label: "v3io-tsdb-${label}", inheritFrom: "${git_project}-${label}", containers: [
                        containerTemplate(name: 'golang', image: 'golang:1.11')
                ]) {
                    node("v3io-tsdb-${label}") {
                        withCredentials([
                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                        ]) {
                            build_v3io_tsdb(MAIN_TAG_VERSION)
                        }
                    }
                }
            },

            'tsdb-nuclio': {
                podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: "${git_project}-${label}") {
                    node("v3io-tsdb-nuclio-${label}") {
                        withCredentials([
                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                        ]) {
                            def NEXT_VERSION

                            stage('get previous release version') {
                                container('jnlp') {
                                    sh """
                                        curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"${git_project_user}\\", name: \\"tsdb-nuclio\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > ~/last_tag;
                                        cat ~/last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > ~/tmp_tag
                                        cat ~/tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > ~/next_version
                                    """
                                    NEXT_VERSION = sh(
                                            script: "cat ~/next_version",
                                            returnStdout: true
                                    ).trim()

                                    echo "$NEXT_VERSION"
                                    next_versions.putAt('tsdb-nuclio', NEXT_VERSION)
                                }
                            }

                            build_nuclio()

                            stage('create tsdb-nuclio prerelease') {
                                container('jnlp') {
                                    sh "curl -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/${git_project_user}/tsdb-nuclio/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\", \"prerelease\": true}'"
                                }
                            }
                        }
                    }
                }
            },

            'demos': {
                podTemplate(label: "demos-${label}", inheritFrom: "${git_project}-${label}") {
                    node("demos-${label}") {
                        withCredentials([
                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                        ]) {
                            def NEXT_VERSION

                            stage('get previous release version') {
                                container('jnlp') {
                                    sh """
                                        curl -H "Authorization: bearer ${GIT_TOKEN}" -X POST -d '{"query": "query { repository(owner: \\"${git_project_user}\\", name: \\"demos\\") { refs(refPrefix: \\"refs/tags/\\", first: 1, orderBy: { field: ALPHABETICAL, direction: DESC }) { nodes { name } } } }" }' https://api.github.com/graphql > last_tag;
                                        cat last_tag | python -c 'import json,sys;obj=json.load(sys.stdin);print obj["data"]["repository"]["refs"]["nodes"][0]["name"]' | sed "s/v//" > tmp_tag
                                        cat tmp_tag | awk -F. -v OFS=. 'NF==1{print ++\$NF}; NF>1{if(length(\$NF+1)>length(\$NF))\$(NF-1)++; \$NF=sprintf("%0*d", length(\$NF), (\$NF+1)%(10^length(\$NF))); print}' > next_version
                                    """

                                    NEXT_VERSION = sh(
                                            script: "cat next_version",
                                            returnStdout: true
                                    ).trim()

                                    echo "$NEXT_VERSION"
                                    next_versions.putAt('demos', NEXT_VERSION)
                                }
                            }

                            build_demo()

                            stage('create demos prerelease') {
                                container('jnlp') {
                                    sh "curl -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/${git_project_user}/demos/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\", \"prerelease\": true}'"
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
                                        next_versions.putAt('prometheus', NEXT_VERSION)
                                    }
                                }

                                build_prometheus(NEXT_VERSION)

                                stage('create prometheus prerelease') {
                                    container('jnlp') {
                                        sh "curl -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" https://api.github.com/repos/${git_project_user}/prometheus/releases -d '{\"tag_name\": \"v${NEXT_VERSION}\", \"target_commitish\": \"master\", \"name\": \"v${NEXT_VERSION}\", \"body\": \"Autorelease, triggered by v3io-tsdb\", \"prerelease\": true}'"
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
            if (PUBLISHED_BEFORE >= expired) {
                echo "Tag too old, published before $PUBLISHED_BEFORE minutes."
            } else {
                echo "${MAIN_TAG_VERSION} is not release tag."
            }
        }
    }

    node("${git_project}-${label}") {
        withCredentials([
                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
            ]) {
            stage('waiting for prereleases moved to releases') {
                container('jnlp') {
                    i = 0
                    def tasks_list = ['demos':null, 'prometheus':null, 'tsdb-nuclio':null]
                    def success_count = 0

                    while( true ) {
                        def done_count = 0

                        echo "attempt #${i}"
                        tasks_list.each { project, status ->
                            if (status == null) {
                                def RELEASE_SUCCESS = sh(
                                        script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/releases/tags/v${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"prerelease\"]' | if grep -iq false; then echo 'release'; else echo 'prerelease'; fi",
                                        returnStdout: true
                                ).trim()

                                echo "${project} is ${RELEASE_SUCCESS}"
                                if (RELEASE_SUCCESS != null && RELEASE_SUCCESS == 'release') {
                                    tasks_list.putAt(project, true)
                                    done_count++
                                    success_count++
                                } else {
                                    def TAG_SHA = sh(
                                            script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/git/refs/tags/v${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"object\"][\"sha\"]'",
                                            returnStdout: true
                                    ).trim()

                                    if(TAG_SHA != null) {
                                        def COMMIT_STATUS = sh(
                                                script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/commits/${TAG_SHA}/statuses | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[0][\"state\"]' | if grep -iq error; then echo 'error'; else echo 'ok'; fi",
                                                returnStdout: true
                                        ).trim()
                                        if (COMMIT_STATUS != null && COMMIT_STATUS == 'error') {
                                            tasks_list.putAt(project, false)
                                            done_count++
                                        }
                                    }
                                }
                            } else {
                                done_count++
                            }
                        }
                        if(success_count >= tasks_list.size()) {
                            echo "all releases have been successfully completed"
                            break
                        }

                        if(done_count >= tasks_list.size() || i++ > attempts) {
                            def failed = []
                            def notcompleted = []
                            def error_string = ''
                            tasks_list.each { project, status ->
                                if(status == null) {
                                    notcompleted += project
                                } else if(status == false) {
                                    failed += project
                                }
                            }
                            if(failed.size()) {
                                error_string += failed.join(',') + ' have been failed :_(. '
                            }
                            if(notcompleted.size()) {
                                error_string += notcompleted.join(',') + ' have been not completed :(. '
                            }
                            error(error_string)
                            break
                        }

                        sleep(60)
                    }
                }
            }

            stage('update release status') {
                container('jnlp') {
                    github.update_release_status(git_project, git_project_user, "v${MAIN_TAG_VERSION}", GIT_TOKEN)
                }
            }
        }
    }

    node("${git_project}-${label}") {
        withCredentials([
                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
        ]) {
            stage('waiting for prereleases moved to releases') {
                container('jnlp') {
                    i = 0
                    def tasks_list = ['demos':null, 'prometheus':null, 'tsdb-nuclio':null]
                    def success_count = 0

                    while( true ) {
                        def done_count = 0

                        echo "attempt #${i}"
                        tasks_list.each { project, status ->
                            if (status == null) {
                                def RELEASE_SUCCESS = sh(
                                        script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/releases/tags/v${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"prerelease\"]' | if grep -iq false; then echo 'release'; else echo 'prerelease'; fi",
                                        returnStdout: true
                                ).trim()

                                echo "${project} is ${RELEASE_SUCCESS}"
                                if (RELEASE_SUCCESS != null && RELEASE_SUCCESS == 'release') {
                                    tasks_list.putAt(project, true)
                                    done_count++
                                    success_count++
                                } else {
                                    def TAG_SHA = sh(
                                            script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/git/refs/tags/v${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"object\"][\"sha\"]'",
                                            returnStdout: true
                                    ).trim()

                                    if(TAG_SHA != null) {
                                        def COMMIT_STATUS = sh(
                                                script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/commits/${TAG_SHA}/statuses | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[0][\"state\"]' | if grep -iq error; then echo 'error'; else echo 'ok'; fi",
                                                returnStdout: true
                                        ).trim()
                                        if (COMMIT_STATUS != null && COMMIT_STATUS == 'error') {
                                            tasks_list.putAt(project, false)
                                            done_count++
                                        }
                                    }
                                }
                            } else {
                                done_count++
                            }
                        }
                        if(success_count >= tasks_list.size()) {
                            echo "all releases have been successfully completed"
                            break
                        }

                        if(done_count >= tasks_list.size() || i++ > attempts) {
                            def failed = []
                            def notcompleted = []
                            def error_string = ''
                            tasks_list.each { project, status ->
                                if(status == null) {
                                    notcompleted += project
                                } else if(status == false) {
                                    failed += project
                                }
                            }
                            if(failed.size()) {
                                error_string += failed.join(',') + ' have been failed :_(. '
                            }
                            if(notcompleted.size()) {
                                error_string += notcompleted.join(',') + ' have been not completed :(. '
                            }
                            error(error_string)
                            break
                        }

                        sleep(60)
                    }
                }
            }
        }
    }
}

