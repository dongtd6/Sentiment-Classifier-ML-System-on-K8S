# -------------------------------FIRE WALL
resource "google_compute_firewall" "firewall_jenkins_port"{
  name = var.firewall_jenkins_port_name
  network = google_compute_network.vpc_tr_network.self_link //"default"
  allow{
    protocol = "tcp"
    ports = var.firewall_jenkins_port_ranges
  }
  source_ranges = var.firewall_jenkins_source_ranges
  depends_on = [google_compute_instance.vm_instance]
}


resource "google_compute_firewall" "firewall_allow_ssh" {
  name    = "allow-ssh-to-vm"
  network = google_compute_network.vpc_tr_network.self_link
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"] 
  target_tags   = ["allow-ssh"] 
  depends_on    = [google_compute_network.vpc_tr_network]
}


resource "google_compute_firewall" "firewall_allow_icmp" {
  name    = "allow-icmp-to-vm"
  network = google_compute_network.vpc_tr_network.self_link
  allow {
    protocol = "icmp"
  }
  source_ranges = ["0.0.0.0/0"] 
  target_tags   = ["allow-icmp"]
  depends_on    = [google_compute_network.vpc_tr_network]
}