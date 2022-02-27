# AQ&U Production 
- Website
- API
- Functions

We use [pipenv](https://pipenv.org) as our python package dependency resolver for local development. 
```bash
pipenv install --no-lock
```

## scripts

### create_gcp_services
`python create_services.py` will run you through the creation of all the services (besides `Functions` and `Cloud Run` because we do these manually)

## BigQuery
`telemetry.telemetry`: All time-series data is stored in this table. Sources are:
- AQ&U: AirU sensors publish data to a Mosquitto MQTT broker hosted at `air.eng.utah.edu` at the University of Utah. A service located at `/home/becnel/aqandu-prod-mosquitto-bq-bridge` on the same server listens to the topic `airu/influx`, ingests incoming data, and forwards it to `telemetry.telemetry`. 

`internal.api_request_tracker`: All user API requests get logged here

`internal.api_quota_history`: Stores the quota used by each API user. 
