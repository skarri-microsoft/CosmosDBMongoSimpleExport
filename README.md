# Cosmos MongoDB Read Write Sample

1. Please update the app.config with connection string
2. Please increase the RUs for collection if you want to ingest data quickly.

Notes:

1. This is guiding sample, doesn't have code for handling failed insertion docs and read docs. Please handle based on your requirement.

2. Use DocsCount and BatchSize to adjust the numbers threads going to write to Cosmos Mongo DB. The same applies to Read scenario also.
 
3. Request Rate Large is an error throws by server to signal the client I have more load than expected so the client has to slowdown and start write again. In this sample wait time gets assign to thead in between minimum and maximum. Please look at MinWait and MaxWait in code.
