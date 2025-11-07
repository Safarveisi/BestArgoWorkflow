# Best of Argo Workflows

## Install Argo Workflows on your K8s cluster

```bash
kubectl create namespace argo # If does not exist
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/download/v3.7.3/install.yaml # Latest release
```

## Create `playground` namespace

```bash
kubectl create namespace playground # Workflows as well as other resources are created in this namespace
```

## Create required resources 

```bash 
kubectl apply -f roles/ # This service account is used in Workflow CRDs
# CAUTION Make sure you have already created the secret `s3-credentials` referenced in the configmap
kubectl apply -f configmap/artifact-repository.yaml # Holds S3 credentials and configurations
```

## Submit a pipeline

### Example
```bash
# Make sure you have argo CLI already installed (see: https://github.com/argoproj/argo-workflows/releases/tag/v3.7.3)
argo submit -n playground pipelines/artifact.yaml --watch
```

### Check the status of your pipeline

```bash
argo get @latest -n playground
```

### Run a spark application, track its progress and delete it

(1) Deploy the Kubeflow Spark operator into your K8s cluster

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
# To run your Spark jobs in a namespace called playground.
# This will also create a serviceaccount in playground namespace and 
# driver pod will use it to communicate with K8s API server (e.g., create executor pods).
helm install spark-operator spark-operator/spark-operator -n spark-operator --set "spark.jobNamespaces={playground}"
```

(2) Run the workflow 

```bash
argo submit -n playground pipelines/k8s-orchestration.yaml --watch
```