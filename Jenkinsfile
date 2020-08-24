label = "${UUID.randomUUID().toString()}"
BUILD_FOLDER = "/go"
attempts=15
git_project = "v3io-tsdb"
git_project_user = "v3io"
git_project_upstream_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"

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
                        sleep(60)
                        
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
            }
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
