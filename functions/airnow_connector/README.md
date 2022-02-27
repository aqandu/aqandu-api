## AQ&U AirNow Connector
This is a bridge between AirNow API and Tetrad BigQuery.

1. A Google Cloud Scheduler Job (`airnow_connector`) fires periodically (every hour), which creates a `pubsub` event message on the topic `trigger_airnow_connector`
2. Query [airnowapi.org](airnowapi.org) using Tom Becnel's key. 
4. The data is sent to the BigQuery table `telemetry.telemetry`

Here are the gcloud commands used to deploy the Function and Scheduler job:

Deploy Scheduler Job:
```bash
gcloud scheduler jobs create pubsub airnow_connector --schedule "0 */1 * * *" --topic trigger_airnow_connector --message-body " "
```
Deploy Function:
```bash
gcloud functions deploy airnow_connector --entry-point main --runtime python38 --trigger-resource trigger_airnow_connector --trigger-event google.pubsub.topic.publish --timeout 540s --env-vars-file .env.yaml --memory 512
```