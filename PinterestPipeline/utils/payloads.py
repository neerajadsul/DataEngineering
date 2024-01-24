from dataclasses import dataclass
from abc import ABC, abstractmethod
import json
import requests

class ApiPayload(ABC):
    endpoint: str
    headers: str
    method: [requests.get, requests.put, requests.post]

    @abstractmethod
    def loader(data, topic) -> str:
        pass


@dataclass
class KinesisPackage(ApiPayload):
    INVOKE_ENDPOINT = 'https://y62ln2mwxb.execute-api.us-east-1.amazonaws.com/test/'
    STREAM_PATH = r'streams/streaming-0a1d8948160f-{topic}/record'
    endpoint = rf'{INVOKE_ENDPOINT}{STREAM_PATH}'
    headers = {'Content-Type': 'application/json'}
    method = requests.put

    def packager(data, topic):
        if 'timestamp' in data:
            data['timestamp'] = data['timestamp'].isoformat()

        if 'date_joined' in data:
            data['date_joined'] = data['date_joined'].isoformat()

        payload = json.dumps({
            "StreamName": f"streaming-0a1d8948160f-{topic}",
            "Data": data,
            "PartitionKey": "partition-1"
        })
        return payload 


@dataclass
class KafkaRestData(ApiPayload):
    INVOKE_ENDPOINT = 'https://y62ln2mwxb.execute-api.us-east-1.amazonaws.com/test/topics/'
    TOPIC_ARG = r'0a1d8948160f.{topic}'
    endpoint = rf'{INVOKE_ENDPOINT}{TOPIC_ARG}'
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    method = requests.post

    def loader(data, topic):
        if 'timestamp' in data:
            data['timestamp'] = data['timestamp'].isoformat()

        if 'date_joined' in data:
            data['date_joined'] = data['date_joined'].isoformat()

        payload = json.dumps({
            "records": [
                {
                    "value": data
                }
            ]
        })
        return payload


class PinterestPayload:
    """Packages JSON data to send via AWS API Gateway endpoint."""
    
    def __init__(self, payloader: ApiPayload):
        self._endpoint = payloader.endpoint
        self._loader = payloader.loader
        self._method = payloader.method
        self._headers = payloader.headers

    def send_data(self, payload, topic) -> requests.Response:
        """Send data to AWS API Endpoint."""
        response = self._method(
            url = self._endpoint.format(topic=topic),
            data = self._loader(payload, topic),
            headers = self._headers,
        )

        return response


if __name__ == "__main__":
    from database import AWSDBConnector
    dbc = AWSDBConnector()
    data = dbc.random_post()
    payload = PinterestPayload(payloader=KinesisPackage)
    for topic, content in data.items():
        print(topic)
        print(content)
        payload.send_data(content, topic)