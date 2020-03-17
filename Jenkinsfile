label = "${UUID.randomUUID().toString()}"
BUILD_FOLDER = "/go"
attempts=15
git_project = "v3io-tsdb"
git_project_user = "v3io"
git_project_upstream_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"


def build_v3io_tsdb(TAG_VERSION) {
    withCredentials([
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'v3io-tsdb'
        stage('prepare sources') {
            container('jnlp') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    git(changelog: false, credentialsId: git_deploy_user_private_key, poll: false, url: "git@github.com:${git_project_user}/${git_project}.git")
                    sh("git checkout ${TAG_VERSION}")
                }
            }
        }

        stage("build ${git_project} binaries in dood") {
            container('golang') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    sh """
                        GO111MODULE=on GOOS=linux GOARCH=amd64 TRAVIS_TAG=${TAG_VERSION} make bin
                        GO111MODULE=on GOOS=darwin GOARCH=amd64 TRAVIS_TAG=${TAG_VERSION} make bin
                        GO111MODULE=on GOOS=windows GOARCH=amd64 TRAVIS_TAG=${TAG_VERSION} make bin
                        ls -la /go/bin
                    """
                }
            }
        }

        stage('upload release assets') {
            container('jnlp') {
                RELEASE_ID = github.get_release_id(git_project, git_project_user, "${TAG_VERSION}", GIT_TOKEN)

                github.upload_asset(git_project, git_project_user, "tsdbctl-${TAG_VERSION}-linux-amd64", RELEASE_ID, GIT_TOKEN)
                github.upload_asset(git_project, git_project_user, "tsdbctl-${TAG_VERSION}-darwin-amd64", RELEASE_ID, GIT_TOKEN)
                github.upload_asset(git_project, git_project_user, "tsdbctl-${TAG_VERSION}-windows-amd64", RELEASE_ID, GIT_TOKEN)
                withCredentials([
                        string(credentialsId: pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[2], variable: 'PACKAGES_ARTIFACTORY_PASSWORD')
                ]) {
                    common.upload_file_to_artifactory(pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[0], pipelinex.PackagesRepo.ARTIFACTORY_IGUAZIO[1], PACKAGES_ARTIFACTORY_PASSWORD, "iguazio-devops/k8s", "tsdbctl-${TAG_VERSION}-linux-amd64")
                }
            }
        }
    }
}

def build_nuclio(V3IO_TSDB_VERSION, internal_status="stable") {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'tsdb-nuclio'
        stage('prepare sources') {
            container('jnlp') {
                if (!fileExists("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")) {
                    sh("cd ${BUILD_FOLDER}; git clone https://${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git src/github.com/${git_project_upstream_user}/${git_project}")
                }
                if ("${internal_status}" == "unstable") {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout development")
                    }
                } else {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout master")
                    }
                }
            }
            parallel(
                    'update tsdb in ingest': {
                        container('jnlp') {
                            dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                sh """
                                    rm -rf functions/ingest/vendor/github.com/${git_project_upstream_user}/v3io-tsdb
                                    git clone https://${GIT_TOKEN}@github.com/${git_project_user}/v3io-tsdb.git functions/ingest/vendor/github.com/${git_project_upstream_user}/v3io-tsdb
                                    cd functions/ingest/vendor/github.com/${git_project_upstream_user}/v3io-tsdb
                                    git checkout ${V3IO_TSDB_VERSION}
                                """
                            }
                        }
                        container('golang') {
                            dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}/functions/ingest/vendor/github.com/${git_project_upstream_user}/v3io-tsdb") {
                                sh """
                                    GO111MODULE=on go mod vendor
                                    rm -rf .git vendor/github.com/nuclio vendor/github.com/${git_project_upstream_user}/frames/vendor/golang.org/x/net vendor/golang.org/x/net
                                """
                                sh("chown 1000:1000 ./ -R")
                            }
                        }
                    },
                    'update tsdb in query': {
                        container('jnlp') {
                            dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                                sh """
                                    rm -rf functions/query/vendor/github.com/${git_project_upstream_user}/v3io-tsdb functions/query/vendor/github.com/${git_project_upstream_user}/v3io-go
                                    git clone https://${GIT_TOKEN}@github.com/${git_project_user}/v3io-tsdb.git functions/query/vendor/github.com/${git_project_upstream_user}/v3io-tsdb
                                    cd functions/query/vendor/github.com/${git_project_upstream_user}/v3io-tsdb
                                    git checkout ${V3IO_TSDB_VERSION}
                                """
                            }
                        }
                        container('golang') {
                            dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}/functions/query/vendor/github.com/${git_project_upstream_user}/v3io-tsdb") {
                                sh """
                                    GO111MODULE=on go mod vendor
                                    rm -rf .git vendor/github.com/nuclio vendor/github.com/${git_project_upstream_user}/frames/vendor/golang.org/x/net vendor/golang.org/x/net
                                    mv vendor/github.com/v3io/v3io-go ${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}/functions/query/vendor/github.com/${git_project_upstream_user}/v3io-go
                                """
                                sh("chown 1000:1000 ./ -R")
                            }
                        }
                    }
            )
        }

        stage('git push') {
            container('jnlp') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        git remote rm origin
                        git remote add origin https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git
                        git add functions/ingest/vendor/github.com functions/query/vendor/github.com;
                    """
                    try {
                        common.shellc("git commit -m 'Updated TSDB to ${V3IO_TSDB_VERSION}'")
                    } catch (err) {
                        echo "Can not commit"
                    }
                    try {
                        if ( "${internal_status}" == "unstable" ) {
                            common.shellc("git push origin development")
                        } else {
                            common.shellc("git push origin master")
                        }
                    } catch (err) {
                        echo "Can not push code"
                    }
                }
            }
            container('golang') {
                sh("rm -rf ${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
            }
        }
    }
}

def build_prometheus(V3IO_TSDB_VERSION, FRAMES_VERSION, internal_status="stable") {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'prometheus'

        stage('prepare sources') {
            container('jnlp') {
                if (!fileExists("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")) {
                    sh("cd ${BUILD_FOLDER}; git clone https://${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git src/github.com/${git_project_upstream_user}/${git_project}")
                }
                if ("${internal_status}" == "unstable") {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout development")
                    }
                } else {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout master")
                    }
                }
            }
            container('golang') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    if("${git_project_user}" != "${git_project_upstream_user}") {
                        sh("GO111MODULE=on go mod edit -replace github.com/${git_project_upstream_user}/v3io-tsdb=github.com/${git_project_user}/v3io-tsdb@${V3IO_TSDB_VERSION}")
                        sh("GO111MODULE=on go mod edit -replace github.com/${git_project_upstream_user}/frames=github.com/${git_project_user}/frames@${FRAMES_VERSION}")
                        sh("GO111MODULE=on go get")
                    } else {
                        sh("GO111MODULE=on go mod edit -replace github.com/${git_project_upstream_user}/v3io-tsdb=github.com/${git_project_upstream_user}/v3io-tsdb@${V3IO_TSDB_VERSION}")
                        sh("GO111MODULE=on go mod edit -replace github.com/${git_project_upstream_user}/frames=github.com/${git_project_upstream_user}/frames@${FRAMES_VERSION}")
                    }
                    sh("GO111MODULE=on go mod vendor")
                    sh("chown 1000:1000 ./ -R")
                }
            }
        }

        stage('git push') {
            container('jnlp') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        git remote rm origin
                        git remote add origin https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git
                        git add go.mod go.sum vendor/modules.txt vendor;
                    """
                    try {
                        common.shellc("git commit -m 'Updated TSDB to ${V3IO_TSDB_VERSION}'")
                    } catch (err) {
                        echo "Can not commit"
                    }
                    try {
                        if ( "${internal_status}" == "unstable" ) {
                            common.shellc("git push origin development")
                        } else {
                            common.shellc("git push origin master")
                        }
                    } catch (err) {
                        echo "Can not push code"
                    }
                }
            }
            container('golang') {
                sh("rm -rf ${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
            }
        }
    }
}

def build_frames(V3IO_TSDB_VERSION, internal_status="stable") {
    withCredentials([
            usernamePassword(credentialsId: git_deploy_user, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME'),
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        def git_project = 'frames'

        stage('prepare sources') {
            container('jnlp') {
                if (!fileExists("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")) {
                    sh("cd ${BUILD_FOLDER}; git clone https://${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git src/github.com/${git_project_upstream_user}/${git_project}")
                }
                if ("${internal_status}" == "unstable") {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout development")
                    }
                } else {
                    dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                        sh("git stash")
                        sh("git checkout master")
                    }
                }
            }
            container('golang') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    if("${git_project_user}" != "${git_project_upstream_user}") {
                        sh("GO111MODULE=on go mod edit -replace github.com/${git_project_upstream_user}/v3io-tsdb=github.com/${git_project_user}/v3io-tsdb@${V3IO_TSDB_VERSION}")
                        sh("GO111MODULE=on go get")
                    } else {
                        sh("GO111MODULE=on go get github.com/${git_project_user}/v3io-tsdb@${V3IO_TSDB_VERSION}")
                    }
                    sh("GO111MODULE=on go mod vendor")
                    sh("chown 1000:1000 ./ -R")
                }
            }
        }

        stage('git push') {
            container('jnlp') {
                dir("${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}") {
                    sh """
                        git config --global user.email '${GIT_USERNAME}@iguazio.com'
                        git config --global user.name '${GIT_USERNAME}'
                        git remote rm origin
                        git remote add origin https://${GIT_USERNAME}:${GIT_TOKEN}@github.com/${git_project_user}/${git_project}.git
                        git add go.mod go.sum vendor/modules.txt;
                    """
                    try {
                        common.shellc("git commit -m 'Updated TSDB to ${V3IO_TSDB_VERSION}'")
                    } catch (err) {
                        echo "Can not commit"
                    }
                    try {
                        if ( "${internal_status}" == "unstable" ) {
                            common.shellc("git push origin development")
                        } else {
                            common.shellc("git push origin master")
                        }
                    } catch (err) {
                        echo "Can not push code"
                    }
                }
            }
            container('golang') {
                sh("rm -rf ${BUILD_FOLDER}/src/github.com/${git_project_upstream_user}/${git_project}")
            }
        }
    }
}

def wait_for_release(V3IO_TSDB_VERSION, next_versions, tasks_list) {
    withCredentials([
            string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
    ]) {
        if (V3IO_TSDB_VERSION != "unstable") {
            stage('waiting for prereleases moved to releases') {
                container('jnlp') {
                    i = 0
                    def success_count = 0

                    while (true) {
                        def done_count = 0

                        echo "attempt #${i}"
                        tasks_list.each { project, status ->
                            if (status == null) {
                                def RELEASE_SUCCESS = sh(
                                        script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/releases/tags/${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"prerelease\"]' | if grep -iq false; then echo 'release'; else echo 'prerelease'; fi",
                                        returnStdout: true
                                ).trim()

                                echo "${project} is ${RELEASE_SUCCESS}"
                                if (RELEASE_SUCCESS != null && RELEASE_SUCCESS == 'release') {
                                    tasks_list.putAt(project, true)
                                    done_count++
                                    success_count++
                                } else {
                                    def TAG_SHA = sh(
                                            script: "curl --silent -H \"Content-Type: application/json\" -H \"Authorization: token ${GIT_TOKEN}\" -X GET https://api.github.com/repos/${git_project_user}/${project}/git/refs/tags/${next_versions[project]} | python -c 'import json,sys;obj=json.load(sys.stdin);print obj[\"object\"][\"sha\"]'",
                                            returnStdout: true
                                    ).trim()

                                    if (TAG_SHA != null) {
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
                        if (success_count >= tasks_list.size()) {
                            echo "all releases have been successfully completed"
                            break
                        }

                        if (done_count >= tasks_list.size() || i++ > attempts) {
                            def failed = []
                            def notcompleted = []
                            def error_string = ''
                            tasks_list.each { project, status ->
                                if (status == null) {
                                    notcompleted += project
                                } else if (status == false) {
                                    failed += project
                                }
                            }
                            if (failed.size()) {
                                error_string += failed.join(',') + ' have been failed :_(. '
                            }
                            if (notcompleted.size()) {
                                error_string += notcompleted.join(',') + ' have been not completed :(. '
                            }
                            error(error_string)
                            break
                        }

                        sleep(60)
                    }
                }
            }
        } else {
            stage('info') {
                echo("Unstable tsdb doesn't trigger tsdb-nuclio and prometheus")
            }
        }
    }
}

podTemplate(label: "${git_project}-${label}", inheritFrom: "jnlp-docker-golang") {
    def MAIN_TAG_VERSION
    def FRAMES_NEXT_VERSION
    def next_versions = ['prometheus':null, 'tsdb-nuclio':null, 'frames':null]

    pipelinex = library(identifier: 'pipelinex@development', retriever: modernSCM(
            [$class:        'GitSCMSource',
             credentialsId: git_deploy_user_private_key,
             remote:        "git@github.com:iguazio/pipelinex.git"])).com.iguazio.pipelinex

    common.notify_slack {
        node("${git_project}-${label}") {
            withCredentials([
                    string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
            ]) {
                stage('get tag data') {
                    container('jnlp') {
                        MAIN_TAG_VERSION = github.get_tag_version(TAG_NAME)

                        echo "$MAIN_TAG_VERSION"
                    }
                }

                if (github.check_tag_expiration(git_project, git_project_user, MAIN_TAG_VERSION, GIT_TOKEN)) {
                    parallel(
                            'v3io-tsdb': {
                                podTemplate(label: "v3io-tsdb-${label}", inheritFrom: "jnlp-docker-golang") {
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
                                podTemplate(label: "v3io-tsdb-nuclio-${label}", inheritFrom: "jnlp-docker-golang") {
                                    node("v3io-tsdb-nuclio-${label}") {
                                        withCredentials([
                                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                                        ]) {
                                            def NEXT_VERSION

                                            if (MAIN_TAG_VERSION != "unstable") {
                                                stage('get previous release version') {
                                                    container('jnlp') {
                                                        CURRENT_VERSION = github.get_short_tag_version("tsdb-nuclio", git_project_user, GIT_TOKEN)
                                                        echo "$CURRENT_VERSION"
                                                        NEXT_VERSION = "${CURRENT_VERSION}-${MAIN_TAG_VERSION}"
                                                        next_versions.putAt("tsdb-nuclio", NEXT_VERSION)
                                                    }
                                                }

                                                build_nuclio(MAIN_TAG_VERSION, "unstable")
                                                build_nuclio(MAIN_TAG_VERSION)

                                                stage('create tsdb-nuclio prerelease') {
                                                    container('jnlp') {
                                                        // development has been triggered when committed to it in github-webhook nuclio function
                                                        // echo "Triggered tsdb-nuclio development will be builded with last tsdb stable version"
                                                        // github.delete_release("tsdb-nuclio", git_project_user, "unstable", GIT_TOKEN)
                                                        // github.create_prerelease("tsdb-nuclio", git_project_user, "unstable", GIT_TOKEN, "development")

                                                        echo "Trigger tsdb-nuclio ${NEXT_VERSION} with tsdb ${MAIN_TAG_VERSION}"
                                                        github.create_prerelease("tsdb-nuclio", git_project_user, NEXT_VERSION, GIT_TOKEN)
                                                    }
                                                }
                                            } else {
                                                stage('info') {
                                                    echo("Unstable tsdb doesn't trigger tsdb-nuclio")
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            'frames': {
                                podTemplate(label: "v3io-frames-${label}", inheritFrom: "jnlp-docker-golang") {
                                    node("v3io-frames-${label}") {
                                        withCredentials([
                                                string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                                        ]) {
                                            def NEXT_VERSION

                                            if (MAIN_TAG_VERSION != "unstable") {
                                                stage('get previous release version') {
                                                    container('jnlp') {
                                                        CURRENT_VERSION = github.get_short_tag_version("frames", git_project_user, GIT_TOKEN)
                                                        echo "$CURRENT_VERSION"
                                                        NEXT_VERSION = "${CURRENT_VERSION}-${MAIN_TAG_VERSION}"
                                                        FRAMES_NEXT_VERSION = NEXT_VERSION
                                                        next_versions.putAt("frames", NEXT_VERSION)
                                                    }
                                                }

                                                build_frames(MAIN_TAG_VERSION, "unstable")
                                                build_frames(MAIN_TAG_VERSION)

                                                stage('create frames prerelease') {
                                                    container('jnlp') {
                                                        // development has been triggered when committed to it in github-webhook nuclio function
                                                        // echo "Triggered frames development will be builded with last tsdb stable version"
                                                        // github.delete_release("frames", git_project_user, "unstable", GIT_TOKEN)
                                                        // github.create_prerelease("frames", git_project_user, "unstable", GIT_TOKEN, "development")

                                                        echo "Trigger frames ${NEXT_VERSION} with tsdb ${MAIN_TAG_VERSION}"
                                                        github.create_prerelease("frames", git_project_user, NEXT_VERSION, GIT_TOKEN)
                                                    }
                                                }
                                            } else {
                                                stage('info') {
                                                    echo("Unstable tsdb doesn't trigger frames")
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                    )
                }
            }
        }

        node("${git_project}-${label}") {
            wait_for_release(MAIN_TAG_VERSION, next_versions, ['tsdb-nuclio': null, 'frames': null])
        }

        // prometheus moved last cos need frames version to build
        podTemplate(label: "v3io-tsdb-prometheus-${label}", inheritFrom: "jnlp-docker-golang") {
            node("v3io-tsdb-prometheus-${label}") {
                withCredentials([
                        string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
                ]) {
                    def TAG_VERSION
                    def NEXT_VERSION

                    if (MAIN_TAG_VERSION != "unstable") {
                        stage('get current version') {
                            container('jnlp') {
                                sh """
                                    cd ${BUILD_FOLDER}
                                    git clone https://${GIT_TOKEN}@github.com/${git_project_user}/prometheus.git src/github.com/prometheus/prometheus
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
                                    CURRENT_VERSION = github.get_current_tag_version("prometheus", git_project_user, GIT_TOKEN)
                                    echo "$CURRENT_VERSION"
                                    version_list=CURRENT_VERSION.split('-')
                                    NEXT_VERSION = "v${TAG_VERSION}-${version_list[1]}-${MAIN_TAG_VERSION}"
                                    echo "$NEXT_VERSION"
                                    next_versions.putAt('prometheus', NEXT_VERSION)
                                }
                            }

                            build_prometheus(MAIN_TAG_VERSION, FRAMES_NEXT_VERSION, "unstable")
                            build_prometheus(MAIN_TAG_VERSION, FRAMES_NEXT_VERSION)

                            stage('create prometheus prerelease') {
                                container('jnlp') {
                                    // development has been triggered when committed to it in github-webhook nuclio function
                                    // echo "Triggered prometheus development will be builded with last tsdb stable version"
                                    // github.delete_release("prometheus", git_project_user, "unstable", GIT_TOKEN)
                                    // github.create_prerelease("prometheus", git_project_user, "unstable", GIT_TOKEN, "development")

                                    echo "Trigger prometheus ${NEXT_VERSION} with tsdb ${MAIN_TAG_VERSION}"
                                    github.create_prerelease("prometheus", git_project_user, NEXT_VERSION, GIT_TOKEN)
                                }
                            }
                        }
                    } else {
                        stage('info') {
                            echo("Unstable tsdb doesn't trigger prometheus")
                        }
                    }
                }
            }
        }

        node("${git_project}-${label}") {
            wait_for_release(MAIN_TAG_VERSION, next_versions, ['prometheus': null])
        }

        node("${git_project}-${label}") {
            withCredentials([
                    string(credentialsId: git_deploy_user_token, variable: 'GIT_TOKEN')
            ]) {
                stage('update release status') {
                    container('jnlp') {
                        github.update_release_status(git_project, git_project_user, "${MAIN_TAG_VERSION}", GIT_TOKEN)
                    }
                }
            }
        }
    }
}
