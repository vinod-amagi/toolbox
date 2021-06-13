# S3 Assets Finder

The tool searches asset in the specified paths in S3 bucket.

```
usage: s3_asset_finder.py [-h] -c CONFIG [-b BUCKET] [-i INPUT] [-p PATHS]
                          [-r RESULTS] [-e EXPIRY]

S3 Asset Finder. The tool searches asset (without extension) from input csv
and prints information.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        config name
  -b BUCKET, --bucket BUCKET
                        AWS S3 Bucket
  -i INPUT, --input INPUT
                        input csv file path that specifies asset ids to search
  -p PATHS, --paths PATHS
                        Paths seperated by comma. e.g. Media/S3,VOD/Media
  -r RESULTS, --results RESULTS
                        Results seperated by
                        comma[asset_id,date,storageclass,size,path,presign].
                        e.g. asset_id,path
  -e EXPIRY, --expiry EXPIRY
                        expiry time
```
>
> Example:
> 
> Presign urls from a path for seven days
>
> python3 s3_asset_finder.py -c profile-1   -r "asset_id,presign" -i vod.csv -p "path/" -e 604800
>  python3 s3_asset_finder.py -c profile-2 -b my-bucket   -r "path" -i sample_input.csv -p "Path1,Path2/"
>
>

> # config.json
>
> AWS S3 Bucket
>
> AWS S3 Access Key
>
> AWS S3 Secret Key
>
