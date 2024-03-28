import requests
import json

def send_message(msg):

    headers = {'content-type': 'application/json'}
    data = {'msgtype': 'text', 'text': {'content': msg}}
    requests.post(url=webhook, headers=headers, data=json.dumps(data))

send_message("测试消\n\n息发送")