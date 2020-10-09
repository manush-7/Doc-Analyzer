import boto3
import json
import time

client = boto3.client('textract')

def startJob(source_bucket_name, file_key_name):
    
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': source_bucket_name,
            'Name': file_key_name
        },
    }
    )
    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

# Document
# boto3 S3 initialization
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    destination_bucket_name = 'testuploadpresign'

    # event contains all information about uploaded object
    print("Event :", event)

    # Bucket Name where file was uploaded
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    print(source_bucket_name)
    # Filename of object (with path)
    file_key_name = event['Records'][0]['s3']['object']['key']
    print(file_key_name)
    # Copy Source Object
    copy_source_object = {'Bucket': source_bucket_name, 'Key': file_key_name}

    jobId = startJob(source_bucket_name, file_key_name)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
        print(response[0])
    #print(response)
    #Dict = {1: ['CE122','CE142'], 2: ['CE242','CE245'], 3: ['CE341','CE342'], 4: ['CE441','CE443']}
    # Initial Dictionary 
    Dict = {1 : {1 : "CE122 MAAP", 2 : "CE142 MAAP"}, 
            2 : {1 : "CE242 MAAP", 2 : "CE245 MAAP"},
            3 : {1 : "CE341 MAAP", 2 : "CE342 MAAP"},
            4 : {1 : "CE441 MAAP", 2 : "CE443 MAAP"}
           }
    Value_item = ""
    STR="CE341 MAAP"
    key=""
    sub_key=""
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
               Value_item = item["Text"]
               print(Value_item)
               for id, info in Dict.items():
                   for k in info:
                       if(Value_item==info[k]):
                           key=k
                           sub_key=info[k]
                           print(key)
                           print(sub_key)
                           break
    
    file_key_name= file_key_name.replace("input/","")   
    file_key_name= str(key)+"/"+sub_key+"/"+file_key_name
    print(file_key_name)

    # S3 copy object operation
    s3_client.copy_object(CopySource=copy_source_object, Bucket=destination_bucket_name, Key=file_key_name)

    return {
       'statusCode': 200,
       'body': json.dumps('S3 events Lambda!')
    }