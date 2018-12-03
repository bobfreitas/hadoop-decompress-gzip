# hadoop-decompress-gzip - MapReduce Job to decompress gzip files before processing

This project provides a reference implementation of how to uncompress gzip files that are being received.
We would want to uncompress gzip files before doing the processing to allow for the contents to be splitable.  
What it does is to chain together two jobs.  The first will run a decompress on the incoming files and then
put the uncompressed files in a temporary location, which will then be used as the input a processing job.
In this case, the processing is simply to reformat the data into the Avro format.  

