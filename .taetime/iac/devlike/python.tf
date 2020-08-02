module "taetime_dev-base_python" {
    source = "../../../../taetime_dev/.taetime/iac/devlike/base_python"

    aws_control_acct_id = local.aws_control_acct_id
    docker_host = var.docker_host
    host_workdir = var.host_workdir
    project_name = var.project_name
    container_name = "python"
    project_data_volume = local.project_data_volume
    container_network = local.container_network
    python_module_entrypoint = "goeatlocals_tools"
}
