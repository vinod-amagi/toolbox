import sys
import getopt
import os, csv, re
import boto3, json
from botocore.errorfactory import ClientError
from botocore.client import Config

try:
    #Python 2.x
    from StringIO import StringIO
except ImportError:
    #Python 3.x
    from io import StringIO
import csv

def get_all(filename):
    with open(filename, "r") as file:
        line = file.readline()
        if re.search("asset.?id", line, re.IGNORECASE):
            reader = csv.reader(file)
        else:
            file.seek(0)
            reader = csv.reader(file)
        items = []
        for row in reader:
            if len(row) > 0:
                items.append(row[0])
        return items

def download_all(account, file, bucket):
    print("download_all::Starting...")
    items = get_all(file)
    for item in items:
        download(account, item, bucket)    
    print("download_all::Exiting...")

def name(uri):
    path, name = os.path.split(uri)
    return name

def transfer_all(account, file, bucket, dest, path):
    print("transfer_all::Starting...")
    items = get_all(file)
    for item in items:
        download(account, item, bucket)
        upload(dest, name(item), path)
    print("transfer_all::Exiting...")


def get_s3uri(uri, bucket):
    if uri.startswith("s3://"):
        return uri
    else:
        return "s3://{0}/{1}".format(bucket, uri)

def download(account, file, bucket):
    cmd = "aws s3 --profile={0} cp {1} ./".format(account, get_s3uri(file, bucket))
    print("download::command \"{0}\"".format(cmd))
    #os.system(cmd)

def upload(account, file, path):
    cmd = "aws s3 --profile={0} cp {1} {2}".format(account, file, path)
    print("upload::command \"{0}\"".format(cmd))
    #os.system(cmd)


def is_available_in_s3(aws_key, aws_secret, bucket, s3_key):
    try:
        S3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        response = S3.head_object(Bucket=bucket, Key= s3_key)
        print("File size in S3 ", int(response['ResponseMetadata']['HTTPHeaders']['content-length']))
        return True
    except ClientError:
        return False

def upload_file(aws_key, aws_secret, bucket, s3_key, localfile):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        response = s3.upload_file(localfile, bucket, s3_key)
        print("response ", response)
        return True
    except ClientError as error:
        print(error)
        return False

def get_s3_client(kwargs):
    if "aws_access_key_id" in kwargs and "aws_secret_access_key" in kwargs:
        s3 = boto3.client('s3', aws_access_key_id=kwargs["aws_access_key_id"], aws_secret_access_key=kwargs["aws_secret_access_key"])
    else:
        s3 = boto3.client('s3')
    return s3

def list_objects(bucket, **kwargs):
    try:
        s3 = get_s3_client(kwargs)

        if "Path" in kwargs:
            path = kwargs["Path"] 
        else:
            path = None

        if path is not None:
            response = s3.list_objects_v2(Bucket=bucket, Delimiter='|', Prefix=path)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Delimiter='|')
        
        if response['KeyCount'] == 0:
            return []
            
        for obj in response["Contents"]:
            yield obj

        while 'NextContinuationToken' in response:
            if path is not None:
                response = s3.list_objects_v2(Bucket=bucket, Delimiter='|', Prefix=path, ContinuationToken=response['NextContinuationToken'])
            else:
                response = s3.list_objects_v2(Bucket=bucket, Delimiter='|', ContinuationToken=response['NextContinuationToken'])
            for obj in response["Contents"]:
                yield obj
    except ClientError as error:
        print(error)

def get_presigned_url(bucket, key, **kwargs):
    if "aws_access_key_id" in kwargs and "aws_secret_access_key" in kwargs:
        s3 = boto3.client('s3', config=Config(signature_version='s3v4'), aws_access_key_id=kwargs["aws_access_key_id"], aws_secret_access_key=kwargs["aws_secret_access_key"])
    else:
        s3 = boto3.client('s3', 'us-east-2', config=Config(signature_version='s3v4'))

    expiry = kwargs["expiry"] if "expiry" in kwargs else 604800
    url = s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expiry)
    return url

def copy_object(src_bucket, src_key, dest_bucket, dest_key, **kwargs):
    if "aws_access_key_id" in kwargs and "aws_secret_access_key" in kwargs:
        s3 = boto3.client('s3', aws_access_key_id=kwargs["aws_access_key_id"], aws_secret_access_key=kwargs["aws_secret_access_key"])
    else:
        s3 = boto3.client('s3')

    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    s3.copy(copy_source, dest_bucket, dest_key)

def get_csv_items(bucket, key, aws_key=None, aws_secret=None):
    try:
        if aws_key and aws_secret:
            s3 = boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        else:
            s3 = boto3.resource('s3')
        csv_object = s3.Object(bucket, key)
        csv_data = csv_object.get()['Body'].read().decode('utf-8')
        file = StringIO(csv_data)
        reader = csv.DictReader(file)

        items = list(map(lambda row : make_item(row, reader.fieldnames), reader))

        return items
    except ClientError as error:
        print(error)
        return False

def make_item(row, fieldnames):
    item = {}
    for field in fieldnames:
        if field in row:
            item[field] = row[field]
    return item

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ha:f:b:d:p:", ["help", "account=", "file=", "bucket=", "dest=","path="])
    except getopt.GetoptError as err:
        # print help information and exit:, 
        print(str(err))
    file = None
    bucket = None
    dest = None
    path = None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
            handled = True
        elif o in ("-a", "--account"):
            account = a
        elif o in ("-f", "--file"):
            file = a
        elif o in ("-b", "--bucket"):
            bucket = a
        elif o in ("-d", "--dest"):
            dest = a
        elif o in ("-p", "--path"):
            path = a
        else:
            assert False, "unhandled option"
    print(account, file, dest, path)
    if account == None or file == None:
        handled = False
    elif dest != None and path != None:
        transfer_all(account, file, bucket, dest, path)
        handled = True
    else:
        download_all(account, file, bucket)
        handled = True
    if not handled:
        usage()
    # ...    

def usage():
    print("Usage\n{0} -a <account> -f <file> [-d] [dest-account] [-p] [path]".format(sys.argv[0]))

config = {}

try:
    with open("config.json", "r") as file:
        config = json.load(file)
except Exception as e:
    #print("No config available", e)
    config = {}

def crude_test():
    _config = config["S3"]["source"]
    copy_object(_config['aws_bucket'], "Media/S3/x.ts", _config['aws_bucket'], "Media/S3/y.ts", aws_access_key_id=_config['aws_access_key'], aws_secret_access_key=_config['aws_secret_key'])
    presigned_url(config['aws_bucket'],  "Media/S3/TBN0119GENERICAMV1CC_mon2.mxf")
    objs = list_objects(config['aws_bucket'], Path="Media/S3", aws_access_key_id=config['aws_access_key'], aws_secret_access_key=config['aws_secret_key'])
    #objs = list_objects(config['aws_bucket'])
    names = list(map(lambda obj: os.path.splitext(os.path.basename(obj["Key"]))[0], objs))
    db = {}
    for name in names: 
        db[name] = { 
                "url": "",
                 "title": name
            }
    print(json.dumps(db, indent=4, sort_keys=True))



if __name__ == "__main__":
    #main()
    crude_test()
