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
### Kafka EC2 Instance
1. Login to EC2 Shell via SSH
2. Install Java 11
3. Install and Configure Kafka 2.8.1
   1. Download from Kafka and extract archive.
   2. Download and add IAM MSK authentication package to `CLASSPATH`
4. Configure EC2 Client Authentication for MSK
   1. Add EC2 Access Role ARN to trust relationship in IAM Roles as a principal.
   2. Update `client.properties` in EC2 Kafka instance configuring bootstrap server.


