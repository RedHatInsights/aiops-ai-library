# Dummy AI service

Serves debugging purposes for the initial AI-Ops pipeline scheme

## Deploy to OpenShift
```
❯ oc new-app ./ --name=aiops-dummu-ai-service --strategy=source
❯ oc start-build aiops-dummu-ai-service --from-dir='.'
```
