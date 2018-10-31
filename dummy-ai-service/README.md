# Dummy AI service

Serves debugging purposes for the initial AI-Ops pipeline scheme

## Deploy to OpenShift

We don't intend to deploy this AI service outside our own playground. Therefore we don't list a Kubernetes
template here. Instead please build as a binary data source directly from this folder.

```
❯ oc new-app ./ --name=aiops-dummu-ai-service --strategy=source
❯ oc start-build aiops-dummu-ai-service --from-dir='.'
```
