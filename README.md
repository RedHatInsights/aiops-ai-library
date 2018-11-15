# AI-Ops: Dummy AI microservice

[![Build Status](https://travis-ci.org/ManageIQ/aiops-dummy-ai-service.svg?branch=master)](https://travis-ci.org/ManageIQ/aiops-dummy-ai-service)
[![License](https://img.shields.io/badge/license-APACHE2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

This is a simple Python web server service. **It does no AI!**. The service is intended for debugging purposes only as an AI microservice placeholder. This dummy AI just forwards the data
from one endpoint to another service.


## Get Started

* Learn about other services within our pipeline
  - [incoming-listener](https://github.com/ManageIQ/aiops-incoming-listener)
  - [data-collector](https://github.com/ManageIQ/aiops-data-collector)
  - [publisher](https://github.com/ManageIQ/aiops-publisher)
* Discover all AI services we're integrating with
  - [dummy-ai](https://github.com/ManageIQ/aiops-dummy-ai-service)
  - [aicoe-insights-clustering](https://github.com/RedHatInsights/aicoe-insights-clustering)
* See deployment templates in the [e2e-deploy](https://github.com/RedHatInsights/e2e-deploy) repository

## Configure

* `NEXT_MICROSERVICE_HOST` - where to pass the processed data (`hostname:port`)

## License

See [LICENSE](LICENSE)
