
terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_network" "devops_network" {
  name = "devops-network"
}

resource "docker_image" "jenkins" {
  name = "jenkins/jenkins:lts"
}

resource "docker_container" "jenkins" {
  name  = "devops-jenkins"
  image = docker_image.jenkins.image_id

  ports {
    internal = 8080
    external = 8081
  }

  volumes {
    host_path      = "/home/aix/jenkins_home"
    container_path = "/var/jenkins_home"
  }
}

output "jenkins_url" {
  value = "http://localhost:8081"
}
