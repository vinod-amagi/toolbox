import os
import csv
import json
import re
import s3
import argparse

def load_config():
    with open("config.json") as file:
        return json.load(file)

def get_inputfiles_from_csv(csv_filename):
    with open(csv_filename, "r") as file:
        line = file.readline()
        if re.search("asset.?id", line, re.IGNORECASE):
            reader = csv.reader(file)
        else:
            file.seek(0)
            reader = csv.reader(file)
        items = []
        for row in reader:
            if len(row) > 0 and len(row[0]) > 0:
                items.append(row[0])
        return items


def get_all_s3_objects(config, bucket, paths):

    if paths is None or paths == "":
        lookup_paths = [None]
    else:
        lookup_paths = list(map(lambda s: s.strip(), paths.split(",")))
    all_items = []
    for path in lookup_paths:
        if path:
            objs = s3.list_objects(bucket,\
                        aws_access_key_id=config['aws_access_key_id'],\
                        aws_secret_access_key=config['aws_secret_access_key'],\
                        Path=path)
        else:
            objs = s3.list_objects(bucket,\
                        aws_access_key_id=config['aws_access_key_id'],\
                        aws_secret_access_key=config['aws_secret_access_key'])

        items = list(map(lambda obj: {
            "asset_id" : os.path.splitext(os.path.basename(obj["Key"]))[0],
            "path" : obj["Key"],
            "date" : obj["LastModified"],
            "size": obj["Size"],
            "storageclass": obj["StorageClass"]
             }, objs))
        all_items.extend(items)

    return all_items

def get_specific_s3_items(config, bucket, input_csv_file, paths):        
    s3_objs = get_all_s3_objects(config, bucket, paths)

    input_items = get_inputfiles_from_csv(input_csv_file)

    s3_filtered_items = {}
    for asset_id in input_items:
        for s3_item in s3_objs:
            if s3_item["asset_id"] == asset_id:
                if asset_id in s3_filtered_items:
                    s3_filtered_items[asset_id].append(s3_item)
                else:
                    s3_filtered_items[asset_id] = [s3_item]
    
    return s3_filtered_items

def get_all_s3_items(config, bucket, paths):
    s3_objs = get_all_s3_objects(config, bucket, paths)

    s3_items = {}
    for s3_obj in s3_objs:
        asset_id = s3_obj["asset_id"]
        if asset_id in s3_items:
            s3_items[asset_id].append(s3_obj)
        else:
            s3_items[asset_id] = [s3_obj]
    
    return s3_items

def print_s3_items(s3_items, req_results):
    req_results_list = list(map(lambda s: s.strip(), req_results.split(",")))

    for asset_id in s3_items:
        for item in s3_items[asset_id]:
            data = {
                "asset_id" : asset_id,
                "item" : item
            }
            row = []
            for result in req_results_list:
                row.append(str(data["item"][result]))
            print(",".join(row))

def require_presign(req_results):
    req_results_list = list(map(lambda s: s.strip(), req_results.split(",")))
    return "presign" in req_results_list

def require_presign_escaped(req_results):
    req_results_list = list(map(lambda s: s.strip(), req_results.split(",")))
    return "presign-esc" in req_results_list


def presign(config, bucket, s3_items, expiry):
 
    for asset_id in s3_items:
        for item in s3_items[asset_id]:
            url = s3.get_presigned_url(bucket, item["path"],\
                            aws_access_key_id=config['aws_access_key_id'],\
                            aws_secret_access_key=config['aws_secret_access_key'],\
                            expiry=expiry
                        )
            item["presign"] = url  

def presign_escaped(config, bucket, s3_items, expiry):
 
    for asset_id in s3_items:
        for item in s3_items[asset_id]:
            url = s3.get_presigned_url(bucket, item["path"],\
                            aws_access_key_id=config['aws_access_key_id'],\
                            aws_secret_access_key=config['aws_secret_access_key'],\
                            expiry=expiry
                        )
            item["presign-esc"] = url.replace("&", "&amp;")        

def process(profile, bucket, input_csv_file, paths, req_results, expiry):
    config = load_config()[profile]

    if bucket is None:
        bucket = config["aws_bucket"]

    if input_csv_file:
        s3_items = get_specific_s3_items(config, bucket, input_csv_file, paths)
    else:
        s3_items = get_all_s3_items(config, bucket, paths)

    if require_presign(req_results):
        presign(config, bucket, s3_items, expiry)

    if require_presign_escaped(req_results):
        presign_escaped(config, bucket, s3_items, expiry)

    print_s3_items(s3_items, req_results)

def main():

    parser = argparse.ArgumentParser(
        description='S3 Asset Finder. The tool searches asset (without extension) from input csv and prints information.')
                
    parser.add_argument(
            '-c', "--config",
            type=str,
            help="config name",
            dest="config",
            required=True)

    parser.add_argument(
            '-b', "--bucket",
            type=str,
            help="AWS S3 Bucket",
            dest="bucket",
            required=False)

    parser.add_argument(
            '-i', "--input",
            help="input csv file path that specifies asset ids to search",
            dest="input",
            default=None,
            required=False)

    parser.add_argument(
            '-p', "--paths",
            help="Paths seperated by comma. e.g. Media/S3,VOD/Media",
            dest="paths",
            default=None,
            required=False)
    
    parser.add_argument(
            '-r', "--results",
            help="Results seperated by comma[asset_id,date,storageclass,size,path,presign]. e.g. asset_id,path",
            dest="results",
            default="path",
            required=False)

    parser.add_argument(
            '-e', "--expiry",
            help="expiry time",
            dest="expiry",
            default=604800,
            required=False)                

    args = parser.parse_args()

    process(args.config, args.bucket, args.input, args.paths, args.results, args.expiry)


if __name__ == "__main__":
    main()
