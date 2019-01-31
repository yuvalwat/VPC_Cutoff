import os
import boto3


class InitializeProperties:

    def __init__(self):
        with open(r"Input\prop.properties") as file_prop:
            _l = [line.split("=") for line in file_prop.readlines()]
            _dict_prop = {key.strip(): value.strip() for key, value in _l}
        self.in_aws_access_key = _dict_prop['aws_access_key']
        self.in_aws_secret_access_key_var = _dict_prop['aws_secret_access_key_var']
        self.in_message = _dict_prop['message']
        self.in_package_directory = os.getcwd()
        self.in_client = boto3.client(
            'sqs',
            region_name=_dict_prop['region_name'],
            aws_access_key_id=self.in_aws_access_key,
            aws_secret_access_key=self.in_aws_secret_access_key_var)
        self.inn_bucketname = _dict_prop['bucketname']
        self.in_user_name = _dict_prop['user_name']
        self.in_host_ip = _dict_prop['host_ip']
        self.in_queueUrl = _dict_prop['queueUrl']
        self.in_batch_command = _dict_prop['batch_command']

    def ret_values(self):
        return (
            self.in_aws_access_key,
            self.in_aws_secret_access_key_var,
            self.in_message,
            self.in_package_directory,
            self.in_client,
            self.inn_bucketname,
            self.in_user_name,
            self.in_host_ip,
            self.in_queueUrl,
            self.in_batch_command
        )
