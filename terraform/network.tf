# --------------------------------------NET WORK
resource "google_compute_network" "vpc_tr_network"{
  name                    = "terraform-network"
  auto_create_subnetworks = "true"
  depends_on = [google_container_cluster.gke-cluster]
}