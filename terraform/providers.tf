// The library with methods for creating and
// managing the infrastructure in GCP, this will
// apply to all the resources in the project
provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone 
}



# Kubernetes provider
provider "kubernetes" {
  host                   = "https://${google_container_cluster.gke-cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.gke-cluster.master_auth[0].cluster_ca_certificate)
  
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "gke-gcloud-auth-plugin" # hoặc "gcloud"  gcloud auth application-default login
    args        = ["get-credentials", google_container_cluster.gke-cluster.name, "--zone", google_container_cluster.gke-cluster.location, "--project", var.project_id]
  }
  //config_path = "~/.kube/config"
}