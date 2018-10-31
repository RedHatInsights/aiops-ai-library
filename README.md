# AI-Ops pipeline

OpenShift deployment scheme for the AI-Ops pipeline

The pipeline is designed to be orchestrated within OpenShift. Therefore, we have a Kubernetes template for each service.
Each service is a separate entity and some microservices are expected to be interchangeble at some point in the future.

## Templates

| Name                      | Filename                          | Purpose                                                | Repository |
| ------------------------- | --------------------------------- | ------------------------------------------------------ | ---------- |
| aiops-globals             | `globals-template.yaml`           | Secrets store for AWS credentials                      |            |
| aiops-incoming-listener   | `incoming-listener-template.yaml` | Kafka listener service, S2I image                      | [ManageIQ/aiops-incoming-listener](https://github.com/ManageIQ/aiops-incoming-listener) |
| aiops-data-collector      | `data-collector-template.yaml`    | Data collector service with public route, Docker image | [ManageIQ/aiops-data-collector](https://github.com/ManageIQ/aiops-data-collector) |
| aiops-publisher           | `publisher-template.yaml`         | Kafka publisher with S3 uploading service, S2I image   | [ManageIQ/aiops-publisher](https://github.com/ManageIQ/aiops-publisher) |

## Dummy AI service

This repo is a home for the `dummy-ai-service`. This is a simple Python web server service. **It does no AI!**.
The service is intended for debugging purposes only as an AI microservice placeholder. This dummy AI just forwards the data
from one endpoint to another service.

## Upload templates to OpenShift
```
❯ oc create -f globals-template.yaml \
            -f incoming-listener-template.yaml \
            -f data-collector-template.yaml \
            -f publisher-template.yaml
```

## Deploy the pipeline
```
❯ oc new-app --template aiops-globals --param AWS_KEY=... --param AWS_SECRET=...
❯ oc new-app --template aiops-incoming-listener
❯ oc new-app --template aiops-data-collector --param AI_MICROSERVICE_HOST=dummy-ai-service:8080
❯ oc new-app --template aiops-publisher
```
