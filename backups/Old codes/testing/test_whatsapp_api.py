import time

import requests

'''
curl -X 'POST' \
  'https://gate.whapi.cloud/messages/text?token=I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0' \
  -H 'Content-Type: application/json' \
  -d '{
  "to": "120363416810610968@g.us",
  "body": "Hello, this message was sent via API!"
}'
'''

HEADERS = {
    'content-type': 'application/json'}

# HEADERS = {
#     "accept": "application/json",
#     "Authorization": "Bearer I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0",
#     "Content-Type": "application/json"
# }


def post_whatsapp_message(Machine_Name):
    url = f'https://gate.whapi.cloud/messages/text?token=I5QPTnzMDJCE8zJP7Qr4ffbtMq6LVxJ0'

    message = (
        "🚨 *MAJOR BREAKDOWN ALERT* 🚨\n\n"
        f"The *{Machine_Name}* has been in breakdown for over 15 minutes.⚠️"
    )

    payload = {
        "to": "120363416810610968@g.us",
        "body": message
    }
    try:
        send_req = requests.post(url, json=payload, headers=HEADERS, timeout=5)
        print(f"post_oee_data:{send_req.status_code}, Request status code")
        send_req.raise_for_status()
    except Exception as e:
        print(f'Error: {e}, while sending the whatsapp message')


if __name__ == '__main__':
    # m_name = ['Cosberg Assy-2', 'Cosberg Assy-3', 'HMT Assy-1', 'HMT Assy-2']
    # for nm in m_name:
    post_whatsapp_message('Cosberg Assy-2')
    time.sleep(0.2)
