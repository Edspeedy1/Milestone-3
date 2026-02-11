from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

# Set the project_id with your project ID
project_id="pubsub-ms1-485721"
topic_name = "designSection"   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

with open("design\\labels.csv") as f:
    labels = f.read().splitlines()
header = labels[0].split(",")
labels = labels[1:]

labelDict = [dict(zip(header, rowLabels.split(","))) for rowLabels in labels] 

idx = 0
while(True):

    msg = labelDict[idx]
    idx += 1
    record_value=json.dumps(msg).replace("'", '"').encode('utf-8')    # serialize the message
    
    try:    
        future = publisher.publish(topic_path, record_value)
        future.result()    
        print("The messages {} has been published successfully".format(msg))
    except: 
        print("Failed to publish the message")
    
    time.sleep(.5)   # wait for 0.5 second
    
    if idx == len(labelDict):
        break
      
