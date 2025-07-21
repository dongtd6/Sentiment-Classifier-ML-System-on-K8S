
// Google Kubernetes Engine
resource "google_container_cluster" "gke-cluster" {
  name     = "${var.project_id}-dev-gke"
  location = var.zone
  initial_node_count = var.initial_node_count
  //remove_default_node_pool = true
  // Enabling Autopilot for this cluster
  //enable_autopilot = false
  node_config {
    machine_type = var.machine_type
    disk_size_gb = var.default_disk_size
  }
}