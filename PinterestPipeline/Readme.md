# Pinterest Data Pipeline
Pinterest performsn daily experiments on historical and daily acquired data to create more value for the customers. Goal of this project is to replicate the workflow using AWS Cloud infrastrcture.

## Emulating User Posts

`user_posting_emulation.py` script connects to a RDS database containing data resembling Pinterest API with a POST request. 

| Table Name | Columns |
|------------|---------|
| `pinterest_data` | `['index', 'unique_id', 'title', 'description', 'poster_name', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'downloaded', 'save_location', 'category']` |
| `geolocation_data` | `['ind', 'timestamp', 'latitude', 'longitude', 'country']`|
| `user_data` | `['ind', 'first_name', 'last_name', 'age', 'date_joined']`|


## Configuration
### SSH Login to EC2 Instance
1. Save identity file `*.pem` locally using secret for SSH connection in Parameter Store in AWS Account.
2. Using public IP, `ec2-user` and identity file above connect via SSH with 
   `ssh -i <identity file> ec2-user@<EC2 instance public IP>`

### Kafka EC2 Instance
1. Login to EC2 Shell via SSH
2. Install Java 11
3. Install and Configure Kafka 2.8.1
   1. Download from Kafka and extract archive.
   2. Download and add IAM MSK authentication package to `CLASSPATH`
4. Configure EC2 Client Authentication for MSK
   1. Add EC2 Access Role ARN to trust relationship in IAM Roles as a principal.
   2. Update `client.properties` in EC2 Kafka instance configuring bootstrap server.

### Connect Kafka to MSK Cluster
1. Copy Kafka connector to your users S3 bucket under `kafka-connect-s3`.
2. Create custom plugin using MSK Connect console with name `<userID>-plugin`.
3. Create MSK Connector with name `<userID>-connector` using following configuration:
   ```
   connector.class=io.confluent.connect.s3.S3SinkConnector
   s3.region=us-east-1
   flush.size=1
   schema.compatibility=NONE
   tasks.max=3
   topics.regex=<YOUR_UUID>.*
   format.class=io.confluent.connect.s3.format.json.JsonFormat
   partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
   value.converter.schemas.enable=false
   value.converter=org.apache.kafka.connect.json.JsonConverter
   storage.class=io.confluent.connect.s3.storage.S3Storage
   key.converter=org.apache.kafka.connect.storage.StringConverter
   s3.bucket.name=<BUCKET_NAME>
   ```