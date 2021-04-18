import boto3
import json
import os

#print (s3)


def list_buckets(s3_client):
    # List buckets
    bucket_response = s3_client.list_buckets()
    print(type(bucket_response))
    # print(bucket_response)
    buckets = bucket_response["Buckets"]
    print(buckets)


def get_client(
    dic_credentials,
    client_type,
    region_name,
    aws_key_id,
    aws_secret, aws_session):

    client = boto3.client(client_type,
                          region_name=region_name,
                          aws_access_key_id= dic_credentials[aws_key_id],
                          aws_secret_access_key=dic_credentials[aws_secret],
                          aws_session_token=dic_credentials[aws_session])
    return client


def get_aws_credentials(
        json_file_name, json_file_aws_env,
        json_file_aws_curr_env,
        lst_aws_keys):
    # Opening file
    json_file = open(json_file_name,)
    json_file_dic = json.load(json_file)

    dic_credentials = {}

    for env in json_file_dic.keys():
        if env == json_file_aws_env:
            for curr_env in json_file_dic[env]:
                for work_env in curr_env.keys():
                    if work_env == json_file_aws_curr_env:
                        dic_credentials = curr_env[work_env]

    key_cnt = 0
    for aws_key in lst_aws_keys:
        for cred_key in dic_credentials.keys():
            if aws_key == cred_key:
                key_cnt += 1

    if key_cnt != len(lst_aws_keys):
        dic_credentials = {}

    return dic_credentials


def main():
    # Constants
    CLIENT_TYPE = 's3'
    REGION_NAME = 'us-east-1'
    AWS_KEY_ID = 'aws_access_key_id'
    AWS_SECRET = 'aws_secret_access_key'
    AWS_SESSION = 'aws_session_token'
    LST_AWS_KEYS = [AWS_KEY_ID, AWS_SECRET, AWS_SESSION]
    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__)) + os.path.sep
    JSON_FILE_NAME = CURRENT_PATH + "aws_credentials.json"
    JSON_FILE_AWS_ENV = 'environments'
    JSON_FILE_AWS_CURR_ENV = 'Default'

    # Reading credential file
    dic_credentials = get_aws_credentials(
        JSON_FILE_NAME, JSON_FILE_AWS_ENV, JSON_FILE_AWS_CURR_ENV, LST_AWS_KEYS)
    #print (dic_credentials)

    s3_client = get_client(dic_credentials, CLIENT_TYPE,
                           REGION_NAME, AWS_KEY_ID, AWS_SECRET, AWS_SESSION)
    #if len(s3_client.exceptions._code_to_exception) > 0:
    #    print("Please check the credentials!")
    #    return

    print(s3_client.get_caller_identity())

    list_buckets(s3_client)


if __name__ == "__main__":
    main()
