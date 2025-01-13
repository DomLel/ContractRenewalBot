""" Basic operations using Slack_sdk """

import os
from slack_sdk import WebClient 
from slack_sdk.errors import SlackApiError 

""" We need to pass the 'Bot User OAuth Token' """
slack_token = os.environ.get("SLACK_BOT_TOKEN")
print(slack_token)

# Creating an instance of the Webclient class
client = WebClient(token=slack_token)

try:
	# Posting a message in #random channel
	response = client.chat_postMessage(
    				channel="nordsec-test",
    				text="Bot's first message")
except SlackApiError as e:
	print(e)
	assert e.response["error"]
	

#
