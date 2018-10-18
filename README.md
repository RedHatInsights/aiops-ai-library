# AI-Ops pipeline

OpenShift deployment scheme for the AI-Ops pipeline

## Upload templates to OpenShift
```
❯ oc create -f globals-template.yaml \
            -f incoming-listener.yaml \
            -f data-collector-template.yaml
```

## Deploy the pipeline
```
❯ oc new-app --template aiops-globals --param AWS_KEY=... --param AWS_SECRET=...
❯ oc new-app --template aiops-incoming-listener
❯ oc new-app --template aiops-data-collector --param AI_MICROSERVICE_HOST=dummy-ai-service:8080
```
