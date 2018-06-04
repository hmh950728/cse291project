package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import surfstore.SurfStoreBasic.FileInfo;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
    protected boolean isleader;

    public MetadataStore(ConfigReader config)
    {

        this.config = config;
	}


	private void start(int port, int numThreads) throws IOException {
        config.getMetadataPort(config.getLeaderNum())
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config.blockPort))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        Map<String,FileInfo> fileMap;

//        private final ManagedChannel metadataChannel;
//        private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;


        public  MetadataStoreImpl(Integer blockPort)
        {
//            this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
//                    .usePlaintext(true).build();
//            this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1",blockPort).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            this.fileMap = new HashMap<String,FileInfo>();
        }
        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver)
        {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver)
        {
            String myfile = request.getFilename();
            // the file does not exist
            if (!fileMap.containsKey(myfile))
            {
                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setVersion(0);
                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }

            else
            {
                FileInfo myinfo = fileMap.get(request.getFilename());
                int myvesion = myinfo.getVersion();
                List<String> mylist = myinfo.getBlocklistList();
                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setFilename(myfile);
                builder.setVersion(myvesion);
                builder.addAllBlocklist(mylist);
                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            }

        }
        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver)
        {
            int myvesion = 0;
            int result;
            int newversion;
            List<String> mylist;
            mylist = getmissingblock(request);
            String myfile = request.getFilename();
            FileInfo myinfo;
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            if (fileMap.containsKey(myfile) && mylist.size() == 0)
            {

                myinfo = fileMap.get(request.getFilename());
                newversion = request.getVersion();
                myvesion = myinfo.getVersion();

                // if the version is not current version +1, return "old version"
                if (newversion != myvesion + 1)
                {
                    result = 1;
                    resultbuilder.setCurrentVersion(myvesion);
                    resultbuilder.setResultValue(result);

                }
                else
                {
                    result = 0;
                    resultbuilder.setCurrentVersion(newversion);
                    resultbuilder.setResultValue(result);


                    FileInfo.Builder filebuilder = FileInfo.newBuilder();
                    filebuilder.setFilename(myfile);
                    filebuilder.setVersion(newversion);
                    filebuilder.addAllBlocklist(request.getBlocklistList());
                    FileInfo mynewinfo = filebuilder.build();
                    fileMap.put(myfile, mynewinfo);


                    // if the size of missing block is 0, return "ok"
//                    if (mylist.size() == 1 && mylist.get(0).equals("0"))
//                    {
//                        result = 0;
//                        resultbuilder.setCurrentVersion(newversion);
//                        resultbuilder.addAllMissingBlocks(mylist);
//                        resultbuilder.setResultValue(result);
//                        // delete the old fileinfo
//                        fileMap.remove(myinfo);
//                        FileInfo.Builder filebuilder = FileInfo.newBuilder();
//                        filebuilder.setFilename(myfile);
//                        // change the version to a new version
//                        filebuilder.setVersion(newversion);
//                        filebuilder.addAllBlocklist(myinfo.getBlocklistList());
//                        FileInfo mynewinfo = filebuilder.build();
//                        // update the filemap
//                        fileMap.put(myfile,mynewinfo);
//
//                    }
//                    else
//                    {




//                    }
                }
            }
            else
            {
                result = 2;
                resultbuilder.setCurrentVersion(0);
                resultbuilder.addAllMissingBlocks(request.getBlocklistList());
                resultbuilder.setResultValue(result);

                myvesion = fileMap.containsKey(myfile)?fileMap.get(myfile).getVersion():0;
                FileInfo.Builder filebuilder = FileInfo.newBuilder();
                filebuilder.setFilename(myfile);
                filebuilder.setVersion(myvesion);
                FileInfo mynewinfo = filebuilder.build();
                fileMap.put(myfile, mynewinfo);
            }
            WriteResult response = resultbuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver)
        {
            int myvesion;
            int result;
            int newversion;
            List<String> mylist;
            String myfile = request.getFilename();
            FileInfo myinfo;
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            if (fileMap.containsKey(myfile))
            {
                myinfo = fileMap.get(request.getFilename());
                newversion = request.getVersion();
                myvesion = myinfo.getVersion();
                mylist = myinfo.getBlocklistList();
                if (newversion != myvesion + 1)
                {
                    result = 1;
                    resultbuilder.setCurrentVersion(myvesion);
                    resultbuilder.setResultValue(result);
                }
                else
                {
                    if (mylist.get(0).equals("0") && mylist.size() == 1)
                    {
                        resultbuilder.setCurrentVersion(newversion);
                        resultbuilder.setResultValue(0);
                    }
                    else
                    {
                        // delete the old fileinfo
                        fileMap.remove(myinfo);
                        FileInfo.Builder filebuilder = FileInfo.newBuilder();
                        filebuilder.setFilename(myfile);
                        // change the version to a new version
                        filebuilder.setVersion(newversion);
                        List<String> mynewlist = new ArrayList<String>();
                        mynewlist.add("0");
                        filebuilder.addAllBlocklist(mynewlist);
                        FileInfo mynewinfo = filebuilder.build();
                        // update the filemap
                        fileMap.put(myfile,mynewinfo);
                        resultbuilder.setCurrentVersion(newversion);
                        resultbuilder.setResultValue(0);
                    }
                }

            }
            else
            {
                resultbuilder.setCurrentVersion(0);
            }
            WriteResult response = resultbuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {

        }
        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver)
        {

        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver)
        {

        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {

        }

        public List<String> getmissingblock(surfstore.SurfStoreBasic.FileInfo request)
        {
            List<String>  missingblock = new  ArrayList<String>();
            List<String> mylist = request.getBlocklistList();
            ListIterator<String> listiter= mylist.listIterator();
            while (listiter.hasNext())
            {
                Block.Builder builder = Block.newBuilder();
                builder.setHash(listiter.next());
                Block response = builder.build();
                if (blockStub.hasBlock(response).getAnswer() == false)
                {
                    missingblock.add(response.getHash());
                }
            }
            return missingblock;
        }


        // TODO: Implement the other RPCs!
    }
}