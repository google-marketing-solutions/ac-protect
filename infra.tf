#################################################################
#   Variables
#################################################################

variable "project_id" {
  type = string
}
variable "project_number" {
  type = string
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "location_id" {
  type    = string
  default = "us-central"
}
variable "location" {
  type    = string
  default = "US"
}
variable "service_name" {
  type    = string
  default = "ac-protect"
}

#################################################################
#   Providers
#################################################################

terraform {

  backend "gcs" {
    prefix  = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

#################################################################
#   Data
#################################################################


data "external" "latest_image_digest" {
  program = ["./fetch_latest_image_digest.sh"]
}

# Attempt to retrieve an existing bucket
data "google_storage_bucket" "existing_config_bucket" {
  name = var.project_id
}

data "google_iam_policy" "public_access" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",
    ]
  }
}

#################################################################
#   BigQuery
#################################################################

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "ac_protect"
  friendly_name               = "ac_protect"
  description                 = "dataset for monitoring your apps marketing connectivity"
  location                    = var.location
}

resource "google_bigquery_table" "collector_gads" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "collector_gads"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "property_id",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "property_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "event_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "last_conversion_date",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "os",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "uid",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF

}


resource "google_bigquery_table" "collector_ga4" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "collector_ga4"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "property_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "os",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "app_version",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "event_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "event_count",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "uid",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "date_added",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "collector_app_store" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "collector_app_store"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "version",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "timestamp",
    "type": "DATETIME",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "collector_play_store" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "collector_play_store"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "version",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "track",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "timestamp",
    "type": "DATETIME",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "alerts" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "alerts"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "app_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "rule_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "trigger",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "trigger_value",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "alert_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "timestamp",
    "type": "DATETIME",
    "mode": "REQUIRED"
  }
]
EOF

}

resource "google_bigquery_table" "last_trigger_log" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "last_trigger_log"

  time_partitioning {
    type = "DAY"
  }

  schema = <<EOF
[
  {
    "name": "name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "timestamp",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
EOF

}


resource "google_bigquery_data_transfer_config" "scheduled_query_app_store" {
  display_name        = "Delete Old Rows"
  data_source_id      = "scheduled_query"
  destination_dataset_id = google_bigquery_dataset.dataset.dataset_id
  schedule            = "every 24 hours"
  disabled            = false
  service_account_name = "${var.project_number}-compute@developer.gserviceaccount.com"
  params = {
    query = <<EOT
      DELETE FROM `${var.project_id}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.collector_app_store.table_id}`
      WHERE TIMESTAMP(timestamp) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);
    EOT
  }
}


resource "google_bigquery_data_transfer_config" "scheduled_query_ga4" {
  display_name        = "Delete Old Rows"
  data_source_id      = "scheduled_query"
  destination_dataset_id = google_bigquery_dataset.dataset.dataset_id
  schedule            = "every 24 hours"
  disabled            = false
  service_account_name = "${var.project_number}-compute@developer.gserviceaccount.com"
  params = {
    query = <<EOT
      DELETE FROM `${var.project_id}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.collector_ga4.table_id}`
      WHERE TIMESTAMP(date_added) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);
    EOT
  }
}


resource "google_bigquery_data_transfer_config" "scheduled_query_play_store" {
  display_name        = "Delete Old Rows"
  data_source_id      = "scheduled_query"
  destination_dataset_id = google_bigquery_dataset.dataset.dataset_id
  schedule            = "every 24 hours"
  disabled            = false
  service_account_name = "${var.project_number}-compute@developer.gserviceaccount.com"
  params = {
    query = <<EOT
      DELETE FROM `${var.project_id}.${google_bigquery_dataset.dataset.dataset_id}.${google_bigquery_table.collector_play_store.table_id}`
      WHERE TIMESTAMP(timestamp) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);
    EOT
  }
}



#################################################################
#   Cloud Run
#################################################################


resource "google_cloud_run_v2_job" "run_job" {
  name     = "${var.service_name}-job"
  location = var.region

  template {
    task_count = 1
    template {
      containers {
        image = "gcr.io/${var.project_id}/${var.service_name}_job@${data.external.latest_image_digest.result["job"]}"
        resources {
          limits = {
            cpu = "1000m"
            memory = "512Mi"
          }
        }
      }
    }
  }
}

resource "google_cloud_scheduler_job" "example_scheduler" {
  name             = "${var.service_name}-job-scheduler"
  schedule         = "0 0 * * *"
  attempt_deadline = "320s"
  region           = "us-central1"
  project          = var.project_id
  retry_config {
    retry_count = 1
  }
  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_number}/jobs/${var.service_name}-job:run"
    oauth_token {
      service_account_email = "${var.project_number}-compute@developer.gserviceaccount.com"
    }
  }
  depends_on = [google_cloud_run_v2_job.run_job]
}



resource "google_cloud_run_service" "run_service" {
  name     = "${var.service_name}-service"
  location = var.region
  metadata {
    annotations = {
      "run.googleapis.com/ingress"          = "all",
    }
  }
  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale" = "0"
        "run.googleapis.com/execution-environment" = "gen2" // Required for setting min instances
      }
    }
    spec {
      containers {
        image = "gcr.io/${var.project_id}/${var.service_name}@${data.external.latest_image_digest.result["service"]}"
        resources {
          limits = {
            cpu = "1000m"
            memory = "512Mi"
          }
        }
        env {
          name = "PROJECT_NAME"
          value = var.project_id
        }
        env {
          name = "PROJECT_NUMBER"
          value = var.project_number
        }
      }
    }
  }
  traffic {
    percent         = 100
    latest_revision = true
  }
}


resource "google_cloud_run_service_iam_policy" "public_access" {
  location    = google_cloud_run_service.run_service.location
  project     = google_cloud_run_service.run_service.project
  service     = google_cloud_run_service.run_service.name
  policy_data = data.google_iam_policy.public_access.policy_data
}


#################################################################
#   Outputs
#################################################################

output "service_url" {
  value = google_cloud_run_service.run_service.status[0].url
}
