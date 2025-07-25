# Ref: https://github.com/terraform-google-modules/terraform-google-kubernetes-engine/blob/master/examples/simple_autopilot_public
# To define that we will use GCP
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.80.0" // Provider version
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  required_version = "1.5.6" // Terraform version
}

data "google_client_config" "default" {}


# --------------------------------------NODE POOL
//resource "google_container_node_pool" "node_pool" {
//  name       = "develop-node-pool"
//  cluster    = "${var.project_id}-dev-gke"
//  location   = var.zone
//  node_count = var.node_count
//  node_config {
//    machine_type = var.machine_type
//    disk_size_gb = var.default_disk_size
//    oauth_scopes = [
//      "https://www.googleapis.com/auth/cloud-platform"
//    ]
//  }
//  depends_on = [google_container_cluster.gke-cluster]
//}

# -------------------------------------
# Grant admin role for default service account in "model-serving" namespace 
resource "kubernetes_cluster_role_binding" "model_serving_admin_binding" {
  metadata {
    name = "model-serving-admin-binding"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = "admin"
  }

  subject {
    kind      = "ServiceAccount"
    name      = "default"
    namespace = "model-serving"
  }
}


