import base64
import os
from google.cloud import storage, dataproc_v1
import time


# Cloud Function entry point
def dataproc_job(event, context):
    # Get the file information from the Cloud Storage event
    bucket_name = event["bucket"]
    file_name = event["name"]
    input_file_path = f"gs://{bucket_name}/{file_name}"
    python_path = f"gs://{bucket_name}/python/main.py"
    region = "us-central1"

    # Spark cluster Creation ------------------------
    cluster_name = "test-cluster"
    # Create a Dataproc client
    dataproc = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create a single-node Dataproc cluster
    cluster = dataproc_v1.Cluster()
    cluster.project_id = os.environ.get("GCP_PROJECT")
    cluster.cluster_name = cluster_name

    cluster_config = dataproc_v1.ClusterConfig()
    cluster_config.master_config.num_instances = 1
    cluster_config.master_config.machine_type_uri = "n2-standard-4"
    cluster_config.master_config.disk_config.boot_disk_size_gb = 500
    cluster_config.master_config.disk_config.boot_disk_type = "pd-standard"
    cluster_config.worker_config.num_instances = 0
    cluster_config.worker_config.machine_type_uri = "n2-standard-4"
    cluster_config.software_config = dataproc_v1.SoftwareConfig(
        image_version="2.2-debian12",
        properties={"dataproc:dataproc.allow.zero.workers": "true"},
        optional_components=[],
    )
    cluster_config.gce_cluster_config.internal_ip_only = False
    cluster_config.gce_cluster_config.zone_uri = "us-central1-a"
    cluster.config = cluster_config

    request = dataproc_v1.CreateClusterRequest(
        project_id=os.environ.get("GCP_PROJECT"),
        region=os.environ.get("DATAPROC_REGION"),
        cluster=cluster,
    )

    cluster = dataproc.create_cluster(request=request)
    cluster_id = cluster.operation.name.split("/")[-1]

    # Spark job submission ------------------
    job_controller = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )
    job_config = dataproc_v1.Job()
    job_config.reference = dataproc_v1.JobReference(
        job_id=f"pyspark-job-{os.environ.get('JOB_NAME_SUFFIX')}"
    )
    job_config.placement = dataproc_v1.JobPlacement(cluster_name=cluster_name)
    job_config.pyspark_job = dataproc_v1.PySparkJob(main_python_file_uri=python_path)

    job = job_controller.submit_job(
        project_id=os.environ.get("GCP_PROJECT"),
        region=os.environ.get("DATAPROC_REGION"),
        job=job_config,
    )
    job_id = job.reference.job_id

    # Delete Cluster after job done -----------------------------
    while True:
        job = job_controller.get_job(
            project_id=os.environ.get("GCP_PROJECT"),
            region=os.environ.get("DATAPROC_REGION"),
            job_id=job_id,
        )
        if job.status.state == dataproc_v1.JobStatus.State.DONE:
            if job.status.state_start_time is not None:
                print(f"Job completed at {job.status.state_start_time}")
            break
        time.sleep(30)
    delete_cluster_request = dataproc_v1.DeleteClusterRequest(
        project_id=os.environ.get("GCP_PROJECT"),
        region=os.environ.get("DATAPROC_REGION"),
        cluster_name=cluster_name,
    )
    operation = dataproc.delete_cluster(request=delete_cluster_request)
    print(f"Cluster deletion operation: {operation.operation.name}")
    print(job_id)
