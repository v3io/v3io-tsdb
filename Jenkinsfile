label = "${UUID.randomUUID().toString()}"
BUILD_FOLDER = "/go"
attempts=15
git_project = "v3io-tsdb"
git_project_user = "v3io"
git_project_upstream_user = "v3io"
git_deploy_user = "iguazio-prod-git-user"
git_deploy_user_token = "iguazio-prod-git-user-token"
git_deploy_user_private_key = "iguazio-prod-git-user-private-key"

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
