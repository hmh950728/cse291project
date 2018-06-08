
This is a java cloud-based file storage application that supports creation, read, update, and deletion of a file. The communications among the servers and clients are through the Google Protocol RPC API. The application allows concurrent clients connection and has fault tolerance with a set of distributed processes. The distributed processes are coordinated with the ideas of replicated state machine and two phase commit. 
The storage application consists of metadata_store and block_store. The metadata_store holds the mapping of filenames to data blocks while the block_store is where the data blocks actually stored. When received a request from the client, the metadata_store divides the data of the file into blocks and only sends the data blocks that are modified to the block_store so as to save some space. Currently the metadata_store can support fault tolerant so there will be multiple metadata_stores running and the shutdown of a minority of them won't affect the performance of the application.


## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
