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
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public final class MetadataStore
{
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;


    public MetadataStore(ConfigReader config)
    {

        this.config = config;
	}


	private void start(int port, int numThreads) throws IOException
    {
        final MetadataStoreImpl metastore;
        // determine whether this client is leader
        //follower
        if ((port != config.getMetadataPort(config.getLeaderNum())))
        {
              metastore = new MetadataStoreImpl(config.blockPort,port);
        }
        // leader
        else
        {
            config.metadataPorts.remove(config.getLeaderNum());
            metastore = new  MetadataStoreImpl(config.blockPort,config.metadataPorts);
        }

        server = ServerBuilder.forPort(port)
                .addService(metastore)
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true)
                {
                    try {
                        if (metastore.isLeader) {
                            metastore.updatecrashedserver();
                        }
                        Thread.sleep(500);
                    }
                    catch (Exception e) {

                    }
                }
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

    public static void main(String[] args) throws Exception
    {
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

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase
    {





        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        private final boolean isLeader;
        private boolean iscrashed;

        private final ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> followers;
        private final MetadataStoreGrpc.MetadataStoreBlockingStub leader;

        private int commit;

        List<log>  locallog = new ArrayList<log>();
        Map<String,FileInfo> fileMap;

        // construct for the leader
        public  MetadataStoreImpl(Integer blockPort,Integer leaderport)
        {
           // start the leader metastore
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1",blockPort).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            this.isLeader = false ;
            this.iscrashed = false ;
            this.fileMap = new HashMap<String,FileInfo>();
            this.leader = MetadataStoreGrpc.newBlockingStub( ManagedChannelBuilder.forAddress("127.0.0.1", leaderport).usePlaintext(true).build());
            this.followers = new ArrayList<>();
        }
        // constructor for the followers
        public  MetadataStoreImpl(Integer blockPort,HashMap<Integer,Integer> metaport)
        {
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1",blockPort).usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            this.isLeader = true ;
            this.iscrashed = false ;
            this.fileMap = new HashMap<String,FileInfo>();
            this.followers = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
            this.leader = null ;
            for (Map.Entry<Integer,Integer > entry : metaport.entrySet())
            {
                int portnum = entry.getValue();
                this.followers.add(MetadataStoreGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("127.0.0.1", portnum).usePlaintext(true).build()));
            }
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
            if (!this.isLeader)
            {
                resultbuilder.setResultValue(3);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
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
//                    fileMap.put(myfile, mynewinfo);
                    two_phase_commit(mynewinfo);

                }
            }
            else
            {
                result = 2;
                resultbuilder.setCurrentVersion(0);
                resultbuilder.addAllMissingBlocks(request.getBlocklistList());
                resultbuilder.setResultValue(result);
//
                myvesion = fileMap.containsKey(myfile)?fileMap.get(myfile).getVersion():0;
                FileInfo.Builder filebuilder = FileInfo.newBuilder();
                filebuilder.setFilename(myfile);
                filebuilder.setVersion(myvesion);
                FileInfo mynewinfo = filebuilder.build();
                two_phase_commit(mynewinfo);
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
            if (!this.isLeader)
            {
                resultbuilder.setResultValue(3);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
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
//                        // delete the old fileinfo
//                        fileMap.remove(myinfo);
                        FileInfo.Builder filebuilder = FileInfo.newBuilder();
                        filebuilder.setFilename(myfile);
                        // change the version to a new version
                        filebuilder.setVersion(newversion);
                        List<String> mynewlist = new ArrayList<String>();
                        mynewlist.add("0");
                        filebuilder.addAllBlocklist(mynewlist);
                        FileInfo mynewinfo = filebuilder.build();
                        // update the filemap
                        two_phase_commit(mynewinfo);
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
            SimpleAnswer response =  SimpleAnswer.newBuilder().setAnswer(this.isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver)
        {
            if (!this.isLeader)
            {
                this.iscrashed = true ;
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver)
        {
            if (!this.isLeader)
            {
                this.iscrashed = false;
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {
            SimpleAnswer response =  SimpleAnswer.newBuilder().setAnswer(this.iscrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        public void vote(surfstore.SurfStoreBasic.Empty request, io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {
            SimpleAnswer response =  SimpleAnswer.newBuilder().setAnswer(this.iscrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
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
        public void update(surfstore.SurfStoreBasic.loglist request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {

//
//            List<log> leaderlog =  new ArrayList<log>();
//            List<log> serverlog =  request.getServerlogList();
//            for (int i=0; i < serverlog.size();i++)
//            {
//                leaderlog.add(serverlog.get(i));
//            }
            // need to add the missing log
            List<log> leaderlog = request.getServerlogList();
            if (leaderlog.size()!=locallog.size())
            {
                for (int  i = locallog.size(); i<leaderlog.size(); i++)
                {
                    log requestlog = leaderlog.get(i);
                    locallog.add(requestlog);
                    FileInfo missingfileinfo = requestlog.getFileinfo();
                    fileMap.put(missingfileinfo.getFilename(),missingfileinfo);
                }
            }
            SimpleAnswer response =  SimpleAnswer.newBuilder().setAnswer(this.iscrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }
        public void commit(surfstore.SurfStoreBasic.log request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver)
        {
            if (!iscrashed)
            {
                locallog.add(request);
                FileInfo missingfileinfo = request.getFileinfo();
                fileMap.put(missingfileinfo.getFilename(),missingfileinfo);
            }
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }
        public void two_phase_commit(surfstore.SurfStoreBasic.FileInfo request)
        {
            log.Builder logbuilder = log.newBuilder().setFileinfo(request);
            log mylog = logbuilder.build();
            locallog.add(mylog);
            int vote = 1;
            //1pc, every followers vote for the commit
            for (int i=0; i<followers.size();i++)
            {
                if (followers.get(i).vote(Empty.newBuilder().build()).getAnswer())
                {
                    vote += 1;
                }

            }
            //2pc if majority vote commit the log
            if (vote > 1.0 * followers.size()/2 )
            {
                fileMap.put(request.getFilename(), request);
                for (int i=0; i<followers.size();i++)
                {
                    if (followers.get(i).isCrashed(Empty.newBuilder().build()).getAnswer()==false)
                    {
                        followers.get(i).commit(mylog);

                    }
                }

            }
            // if the vote failed
            else
            {
                locallog.remove(request);

            }

        }
        public void updatecrashedserver()
        {
            SimpleAnswer response;
            loglist.Builder logbuilder = loglist.newBuilder();
            for (int i=0;i<locallog.size();i++)
            {
                logbuilder.addServerlog(i,locallog.get(i));

            }
            loglist alllog= logbuilder.build();
            for (int i=0; i< followers.size();i++)
            {
                if(followers.get(i).isCrashed(Empty.newBuilder().build()).getAnswer()== false)
                {
                    response = followers.get(i).update(alllog);

                }
            }
        }

        // TODO: Implement the other RPCs!
    }
}