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

public final class MetadataStore
{
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
    protected   ManagedChannel blockChannel;
    protected   BlockStoreGrpc.BlockStoreBlockingStub blockStub;


    public MetadataStore(ConfigReader config)
    {

        this.config = config;
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1",config.getBlockPort()).usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

	}

	private void start(int port, int numThreads) throws IOException
    {
        final MetadataStoreImpl metastore;
        // determine whether this client is leader
        //follower constructor for metadata
        if ((port != config.getMetadataPort(config.getLeaderNum())))
        {
              metastore = new MetadataStoreImpl(config.blockPort,port);
        }
        // leader constructor for metadata
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
        // sleep 500 miles to update the log
        new Thread(new Runnable(){
            @Override
            public void run() {
                while (true)
                {
                    try {
                        if (metastore.isLeader)
                        {
                            metastore.updatecrashedserver();
                        }
                        Thread.sleep(500);
                    }
                    catch (Exception e)
                    {

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

    class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase
    {

        private  boolean isLeader;
        private boolean iscrashed;

        private  ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> followers;
        private  MetadataStoreGrpc.MetadataStoreBlockingStub leader;

        List<log>  locallog = new ArrayList<log>();
        Map<String,FileInfo> fileMap;

        // construct for the leader
        public  MetadataStoreImpl(Integer blockPort,Integer leaderport)
        {
           // start the leader metastore
            this.isLeader = false ;
            this.iscrashed = false ;
            this.fileMap = new HashMap<String,FileInfo>();
            this.leader = MetadataStoreGrpc.newBlockingStub( ManagedChannelBuilder.forAddress("127.0.0.1", leaderport).usePlaintext(true).build());
            this.followers = new ArrayList<>();
        }
        // constructor for the followers
        public  MetadataStoreImpl(Integer blockPort,HashMap<Integer,Integer> metaport)
        {
            this.isLeader = true ;
            this.iscrashed = false ;
            this.fileMap = new HashMap<String,FileInfo>();
            this.followers = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
            this.leader = null;
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
            String filename = request.getFilename();
            // the file does not exist
            if (!fileMap.containsKey(filename))
            {
                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setVersion(0);
                builder.setFilename(filename);
                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            // return the information in the file map
            else
            {
                FileInfo fileinfoinmap = fileMap.get(request.getFilename());
                FileInfo.Builder builder = FileInfo.newBuilder();
                builder.setFilename(filename);
                builder.setVersion(fileinfoinmap.getVersion());
                builder.addAllBlocklist(fileinfoinmap.getBlocklistList());
                FileInfo response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            }
        }
        @Override
        synchronized public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver)
        {
            int versioninmap;
            int result;
            int newversion;
            List<String> missingblock;
            missingblock = getmissingblock(request);
            String filename = request.getFilename();
            FileInfo fileinfoinmap;
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            if (!this.isLeader)
            {
                resultbuilder.setResultValue(3);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            newversion = request.getVersion();
            versioninmap  = fileMap.containsKey(filename)?fileMap.get(filename).getVersion():0;
            // if the version is not current version +1, return "old version"
            if (newversion !=versioninmap  + 1)
            {
                result = 1;
                resultbuilder.setCurrentVersion(versioninmap);
                resultbuilder.setResultValue(result);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;

            }
            if (missingblock.size() == 0)
            {
                // when there is the no missing block and version is valid
                FileInfo.Builder filebuilder = FileInfo.newBuilder();
                filebuilder.setFilename(filename);
                filebuilder.setVersion(newversion);
                filebuilder.addAllBlocklist(request.getBlocklistList());
                FileInfo infotocommit = filebuilder.build();
                // 2pc commit the update
                two_phase_commit(infotocommit);
                // send back the result
                result = 0;
                resultbuilder.setCurrentVersion(newversion);
                resultbuilder.setResultValue(result);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            else
            {
                result = 2;
                resultbuilder.setCurrentVersion(versioninmap);
                resultbuilder.addAllMissingBlocks(missingblock);
                resultbuilder.setResultValue(result);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
        }

        @Override
        synchronized public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver)
        {

            int versioninmap ;
            int result;
            int newversion;
            List<String> blocklistinmap;
            String filename = request.getFilename();
            FileInfo fileinfoinmap;
            WriteResult.Builder resultbuilder = WriteResult.newBuilder();
            // if this server is not leader, return "Not Leader"
            if (!this.isLeader)
            {
                resultbuilder.setResultValue(3);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            if (fileMap.containsKey(filename))
            {
                fileinfoinmap = fileMap.get(request.getFilename());
                newversion = request.getVersion();
                versioninmap  = fileinfoinmap .getVersion();
                blocklistinmap =fileinfoinmap .getBlocklistList();
                //  when the version is not old version+1, return old version
                if (newversion != versioninmap + 1)
                {
                    result = 1;
                    resultbuilder.setCurrentVersion(versioninmap);
                    resultbuilder.setResultValue(result);
                    WriteResult response = resultbuilder.build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                else
                {
                    // when the file has been deleted, return old version
                    if (request.getBlocklistCount()==1&&request.getBlocklist(0).equals("0"))
                    {
                        resultbuilder.setCurrentVersion(versioninmap);
                        resultbuilder.setResultValue(0);
                        WriteResult response = resultbuilder.build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                    else
                    {

                        FileInfo.Builder filebuilder = FileInfo.newBuilder();
                        filebuilder.setFilename(filename);
                        // change the version to a new version
                        filebuilder.setVersion(newversion);
                        List<String> allreadydelete = new ArrayList<String>();
                        // put "0" to show that the file has already been deleted
                        allreadydelete.add("0");
                        filebuilder.addAllBlocklist(allreadydelete);
                        FileInfo infotocommit = filebuilder.build();
                        // 2pc commit the update
                        two_phase_commit(infotocommit);
                        // send back the result
                        resultbuilder.setCurrentVersion(newversion);
                        resultbuilder.setResultValue(0);
                        WriteResult response = resultbuilder.build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                }

            }
            // if the file name is not in filemap, set the version 0
            else
            {
                resultbuilder.setCurrentVersion(0);
                WriteResult response = resultbuilder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
        }
        @Override
        synchronized public void getVersion(surfstore.SurfStoreBasic.FileInfo request,io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver)
        {
            FileInfo fileinfoinmap = fileMap.get(request.getFilename());
            int versioninmap  = fileinfoinmap .getVersion();
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(request.getFilename());
            builder.setVersion(versioninmap);
            FileInfo response = builder.build();
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
        @Override
        public void vote(surfstore.SurfStoreBasic.Empty request, io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {
            SimpleAnswer response =  SimpleAnswer.newBuilder().setAnswer(!this.iscrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public List<String> getmissingblock(surfstore.SurfStoreBasic.FileInfo request)
        {
            List<String>  missingblock = new  ArrayList<String>();
            List<String> blocklistinmap = request.getBlocklistList();
            for (int i=0; i< blocklistinmap.size();i++)
            {
                Block.Builder builder = Block.newBuilder();
                builder.setHash(blocklistinmap.get(i));
                Block response = builder.build();
                if (blockStub.hasBlock(response).getAnswer() == false)
                {
                    missingblock.add(response.getHash());
                }
            }
            return missingblock;
        }
        @Override
        synchronized public void update(surfstore.SurfStoreBasic.loglist request,
                           io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver)
        {

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
        @Override
        synchronized public void commit(surfstore.SurfStoreBasic.log request,
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
        synchronized public void two_phase_commit(surfstore.SurfStoreBasic.FileInfo request)
        {

            log.Builder logbuilder = log.newBuilder().setFileinfo(request);
            log mylog = logbuilder.build();
            locallog.add(mylog);
            int vote = 1;
            //1pc, every followers vote for the commit
            for (int i=0; i< followers.size();i++)
            {
                if (followers.get(i).vote(Empty.newBuilder().build()).getAnswer())
                {
                    vote += 1;
                }

            }
            //2pc if majority vote commit the log
            if (vote > 1.0 * followers.size()/2 )
            {
                this.fileMap.put(request.getFilename(), request);
                for (int i=0; i < followers.size();i++)
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
            // wait 500ms to update the log for uncrashed server
            loglist.Builder logbuilder = loglist.newBuilder();
            for (int i=0;i < locallog.size();i++)
            {
                logbuilder.addServerlog(i,locallog.get(i));

            }
            loglist alllog= logbuilder.build();
            for (int i=0; i < followers.size();i++)
            {
                if(followers.get(i).isCrashed(Empty.newBuilder().build()).getAnswer() == false)
                {
                    followers.get(i).update(alllog);
                }
            }
        }
        // TODO: Implement the other RPCs!
    }
}