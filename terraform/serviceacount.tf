
# ---------------------------Jenkins Service Account
# Create and permission for Jenkins Service Account
resource "kubernetes_namespace" "jenkins_namespace" {
  metadata {
    name = "jenkins" # a namspace only for service account
    labels = {
      environment = "devops-tools"
    }
  }
  depends_on = [google_container_cluster.gke-cluster]
}
# Service Account for Jenkins Master
resource "kubernetes_service_account" "jenkins_master_sa" {
  metadata {
    name      = "jenkins" # serivce account is 'jenkins'
    namespace = kubernetes_namespace.jenkins_namespace.metadata[0].name
  }
  depends_on = [kubernetes_namespace.jenkins_namespace]
}
# Secret Token for Jenkins Master SA
resource "kubernetes_secret" "jenkins_master_sa_token" {
  metadata {
    name      = "jenkins-sa-token" # secret name include token
    namespace = kubernetes_service_account.jenkins_master_sa.metadata[0].namespace
    annotations = {
      "kubernetes.io/service-account.name" = kubernetes_service_account.jenkins_master_sa.metadata[0].name
    }
  }
  type = "kubernetes.io/service-account-token"
  depends_on = [kubernetes_service_account.jenkins_master_sa]
}

# ClusterRole grant for Jenkins SA overal cluster (to managerment Pods on it's namespace
# and deploy on other namespace). This is ClusterRole so don't need namespace.
resource "kubernetes_cluster_role" "jenkins_cluster_role" {
  metadata {
    name = "jenkins-gke-controller-clusterrole" # ClusterRole name
  }
  rule {
    api_groups = [""] # Core API group
    resources  = ["pods", "pods/exec", "pods/log"] # agent Pods managerment
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
  rule {
    api_groups = [""] # Core API group cho deploy
    resources  = ["services", "configmaps", "secrets", "events"] # Quyền deploy ứng dụng (secrets cho Helm)
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
  rule {
    api_groups = ["apps"] # API group cho Deployments, StatefulSets
    resources  = ["deployments", "statefulsets", "replicasets"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
  rule {
    api_groups = ["networking.k8s.io"] # API group cho Ingress
    resources  = ["ingresses"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
  rule {
    api_groups = ["helm.sh"]
    resources  = ["releases"]
    verbs      = ["create", "delete", "get", "list", "patch", "update", "watch"]
  }
}

# ClusterRoleBinding to bind the above ClusterRole to Jenkins' Service Account
# ClusterRoleBinding don't need namespace because it is cluster level.
resource "kubernetes_cluster_role_binding" "jenkins_cluster_role_binding" {
  metadata {
    name = "jenkins-gke-controller-clusterrolebinding"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.jenkins_cluster_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.jenkins_master_sa.metadata[0].name
    namespace = kubernetes_service_account.jenkins_master_sa.metadata[0].namespace
  }
  depends_on = [
    kubernetes_service_account.jenkins_master_sa,
    kubernetes_cluster_role.jenkins_cluster_role
  ]
}
