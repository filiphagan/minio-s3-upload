# Upload a file to S3 using MinIO

## Description
The script uploads a chosen file into S3-like object storage server.
After the successful upload *checksum* operation is performed to check the binary integrity of the data.

About [MinIO](https://min.io/):
```
MinIO offers high-performance, S3 compatible object storage.
Native to Kubernetes, MinIO is the only object storage suite available on
every public cloud, every Kubernetes distribution, the private cloud and the
edge. MinIO is software-defined and is 100% open source under GNU AGPL v3.
```

In this example I'm running the MinIO server locally at port 9000. 
Launch the server and create a public bucket before running the script.

--------------------------------
Requirements: python 3.10+

Files:
- s3_upload.py : the script
- requirements.txt : dependencies
- README.md

## Script execution

Parameters: 
<pre>
--s3url    (str) : default "localhost:9000", host address. MioIO accepts "host:port" format
--s3key    (str) : default "minioadmin", access key
--s3secret (str) : default "minioadmin", secret key
--bucket   (str) : bucket name
--s3path   (str) : output file path on S3 bucket
-f --file  (str) : local file path
</pre> 

Example:
```
python s3_upload.py \
    --s3url localhost:9000 \
    --s3key "admin"
    --s3secret "pswrd"
    -f "./requirements.txt" \
    --bucket "mlops-bucket-513905719350275"
    --s3path "/s3/object/path/requirements.txt"
```
