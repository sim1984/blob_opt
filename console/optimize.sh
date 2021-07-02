# /bin/bash
./BlobOpt --database=localhost:NEWDEMO --user=SYSDBA --password=masterkey --table=wnppaper --blobfield=paper --optimize --rows=10 --blobType=segmented --segmentSize=32000
