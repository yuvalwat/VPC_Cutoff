from boto.s3.key import Key
import boto.s3.connection
from clint.textui import colored
import paramiko
import os

from Initialize_SendToSQS import InitializeProperties


def send_sqs_to_queue(in_client, in_message, in_queue_url):
    print(colored.green('Sending to SQS..'))
    try:
        sqs_response = in_client.send_message(
            QueueUrl=in_queue_url, MessageBody=in_message)
    except UnboundLocalError:
        print(colored.red("Failed in sending to SQS queue"))

    return sqs_response


def run_batch_command(host, u_name, k_file_name, in_batch_command):
    print(colored.green('SSH attmept to connect to batch machine..'))
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=u_name, key_filename=k_file_name)
    print(colored.green('SSH connection established..'))
    print(colored.green('Starting batch..'))
    stdin, stdout, stderr = ssh.exec_command(in_batch_command)
    print(colored.yellow(stderr.readlines()))
    ssh.close()
    print(colored.green('Batch run finished successfully'))


def delete_s3_objects(
        in_aws_access_key,
        in_aws_secret_access_key_var,
        in_bucket_name):
    conn = boto.s3.connect_to_region(
        'us-east-1',
        aws_access_key_id=in_aws_access_key,
        aws_secret_access_key=in_aws_secret_access_key_var,
        is_secure=True,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )

    bucket = conn.get_bucket(in_bucket_name)
    for key in bucket.list(prefix='batch/UAT/output/'):
        print(colored.green(f"Deleting key...{key}"))
        key.delete()


def main():
    properties_class = InitializeProperties()
    aws_access_key, aws_secret_access_key_var, message, package_directory, client, bucketname,\
        user_name, host_ip, queue_url, batch_command = properties_class.ret_values()
    delete_s3_objects(aws_access_key, aws_secret_access_key_var, bucketname)
    re = send_sqs_to_queue(client, message, queue_url)
    if re["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print(colored.green('Sent to SQS successfully..'))
    else:
        raise Exception(colored.red("Failed in sending to SQS queue"))
    run_batch_command(
        host_ip,
        user_name,
        os.path.join(package_directory, 'Input', 'Yuval.pub'),
        batch_command)


if __name__ == "__main__":
    main()
