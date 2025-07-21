
# -------------------------------NAME SPACE
resource "kubernetes_namespace" "model_serving_namespace" {
  metadata {
    name = "model-serving"
    labels = {
      environment = "development"
    }
  }
  depends_on = [google_container_cluster.gke-cluster]
}

resource "kubernetes_namespace" "monitoring_namespace" {
  metadata {
    name = "monitoring"
    labels = {
      environment = "development"
    }
  }
  depends_on = [google_container_cluster.gke-cluster]
}

resource "kubernetes_namespace" "logging_namespace" {
  metadata {
    name = "logging"
    labels = {
      environment = "development"
    }
  }
  depends_on = [google_container_cluster.gke-cluster]
}

resource "kubernetes_namespace" "tracing_namespace" {
  metadata {
    name = "tracing"
    labels = {
      environment = "development"
    }
  }
  depends_on = [google_container_cluster.gke-cluster]
}