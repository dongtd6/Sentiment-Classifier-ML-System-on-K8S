# ------------------------------BUCKET
resource "google_storage_bucket" "new-bucket" {
  name          = var.bucket
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  depends_on = [google_container_cluster.gke-cluster]
}