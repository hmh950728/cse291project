package surfstore;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.*;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    private  final Map<String,byte[]> localblock;

    public Client(ConfigReader config)
    {

        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        this.localblock = new HashMap<String,byte[]>();
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    private void ensure(boolean b)
    {
        if (b == false)
        {
            throw new RuntimeException("Assertion failed");
        }
    }
    private  static Block bytetoBlock(byte[] s)
    {
        Block.Builder builder = Block.newBuilder();
        try
        {
            builder.setData(ByteString.copyFrom(s.toString(),"UTF-8"));
        } catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        builder.setHash(HashUtils.sha256(s));
        return builder.build();
    }
    // use to read the file in local and create a lot of blocks
    private ArrayList<byte[]>createblock(String filename)
    {
        int Block_size= 4096;
        File f = new File(filename);
        ArrayList<byte[]> myblocklist= new ArrayList<byte[]>();
        try (FileInputStream fis = new FileInputStream(f))
        {
            byte[] block = new byte[Block_size];
            while (fis.read(block) >= 0)
            {
                {
                    myblocklist.add(block);
                }
            }
            localblock.put(HashUtils.sha256(block), block);

        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return myblocklist;
    }
    // upload the file
    private void upload(String filename)
    {
        //before upload get a hashlist of the block
        ArrayList<byte[]> fileblock = createblock(filename);
        List<String> blocklist =  new ArrayList<String>();
        for (int i=0; i < fileblock.size();i++)
        {
           blocklist.add(bytetoBlock(fileblock.get(i)).getHash());
        }
       // first read the file in metastore
        FileInfo.Builder filebuilderfirst = FileInfo.newBuilder();
        String [] splitFileName  =  filename.split("/");
        String fileName =  splitFileName[splitFileName.length - 1];
        filebuilderfirst.setFilename(fileName);
        FileInfo readinfo= filebuilderfirst.build();

       // get the result of the read
        FileInfo readresult = metadataStub.readFile(readinfo);

        // request the modification of the file
        FileInfo.Builder requestbuilder = FileInfo.newBuilder();
        requestbuilder.setFilename(fileName);
        requestbuilder.setVersion(readresult.getVersion());
        requestbuilder.addAllBlocklist(blocklist);
        FileInfo request = requestbuilder.build();


        WriteResult modifyresult = metadataStub.modifyFile(request);
        // if update is not successful
        while (modifyresult.getResultValue()!= 0)
        {
            int result = modifyresult.getResult().getNumber();
            // update the version
            int version = modifyresult.getCurrentVersion()+ 1 ;
            // if missing block
            if (result == 2)
            {
                List<String> missingblock = modifyresult.getMissingBlocksList();
                for (int i=0; i < missingblock.size();i++)
                {
                    Block.Builder bbuilder = Block.newBuilder();
                    bbuilder.setHash(missingblock.get(i));
                    bbuilder.setData(ByteString.copyFrom(localblock.get(missingblock.get(i))));
                    blockStub.storeBlock(bbuilder.build());
                }

            }
            requestbuilder = FileInfo.newBuilder();
            requestbuilder.setFilename(fileName);
            requestbuilder.setVersion(version);
            requestbuilder.addAllBlocklist(blocklist);
            request = requestbuilder.build();
            modifyresult = metadataStub.modifyFile(request);
        }
        System.out.println("Ok");
    }
    private void download(String filename,String download)
    {
        //
        List<String> fileblocklist =  new ArrayList<String>();
        List<String> blockneeddownload = new ArrayList<String>();
        //
        // first read the file in metastore
        FileInfo.Builder filebuilderfirst = FileInfo.newBuilder();
        filebuilderfirst.setFilename(filename);
        FileInfo readinfo = filebuilderfirst.build();

        // get the result of the read
        FileInfo readresult = metadataStub.readFile(readinfo);

        if (readresult.getVersion()== 0||(readresult.getBlocklistCount()==1&&readresult.getBlocklist(0).equals("0")))
        {
            System.out.println("Not Found");
            return;
        }
        else
         {
             System.out.println("Ok");
             fileblocklist = readresult.getBlocklistList();
            ListIterator<String> filelistiter = fileblocklist.listIterator();
            // find all the block need to be downloaded
            for (int i = 0; i < fileblocklist.size(); i++)
            {
                if (!localblock.containsKey(fileblocklist.get(i)))
                {
                    blockneeddownload.add(fileblocklist.get(i));
                }
            }
            // download the block data to localblock map
            for (int i = 0; i < blockneeddownload.size(); i++) {
                Block.Builder block_builder = Block.newBuilder();
                block_builder.setHash(blockneeddownload.get(i));
                Block block = block_builder.build();
                SimpleAnswer answer = blockStub.hasBlock(block);
                if (answer.getAnswer())
                {
                    byte[] blockdata = blockStub.getBlock(block).getData().toByteArray();
                    localblock.put(blockneeddownload.get(i), blockdata);
                }
         }
            //Write the file
            File f = new File(download + "/" + filename);
            OutputStream os = null;
            try
            {
                os = new FileOutputStream(f);
                for (int i = 0; i < fileblocklist.size(); i++)
                {
                    os.write(localblock.get(fileblocklist.get(i)));
                }
                os.close();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    private void delete(String filename)
    {
        WriteResult deleteresult;
        // first read the file in metastore
        do {
            FileInfo.Builder filebuilderfirst = FileInfo.newBuilder();
            filebuilderfirst.setFilename(filename);
            FileInfo readinfo = filebuilderfirst.build();


            // get the result of the read
            FileInfo readresult = metadataStub.readFile(readinfo);
            // check if the file exsits or the file has been deleted
            int version = readresult.getVersion();

            if (readresult.getVersion() == 0 || (readresult.getBlocklistCount() == 1 && readresult.getBlocklist(0).equals("0"))) {
                System.out.println("Not Found");
                return;
            } else
                {
                // update the version info;
                filebuilderfirst.setVersion(version + 1);
                deleteresult = metadataStub.deleteFile(filebuilderfirst.build());

            }
        }
        while(deleteresult.getResult() == WriteResult.Result.OLD_VERSION);
        System.out.println("OK");
    }
    private int getversion(String filename)
    {
        // first read the file in metastore
        FileInfo.Builder filebuilderfirst = FileInfo.newBuilder();
        filebuilderfirst.setFilename(filename);
        FileInfo readinfo = filebuilderfirst.build();
        FileInfo readresult = metadataStub.readFile(readinfo);
        //
        System.out.println(readresult.getVersion());
        return readresult.getVersion();
    }

    private void go(String operation, String filePath, String downPath)
    {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        switch (operation)
        {
            case "download":
                download(filePath, downPath);
                logger.info("download finish");
                break;
            case "upload":
                upload(filePath);
                logger.info("upload finish");
                break;
            case "delete":
                delete(filePath);
                logger.info("delete finish");
                break;
            case "getversion":
                getversion(filePath);
                break;
        }

        // TODO: Implement your client here
	}

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args)
    {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        if(args.length > 1) {
            parser.addArgument("operation").type(String.class)
                    .help("Operation of client");
            parser.addArgument("filePath").type(String.class)
                    .help("Path to client target");
        }
        if (args.length > 3)
            parser.addArgument("downloadDir").type(String.class)
                    .help("Directory of download location");
        Namespace res = null;
        try
        {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e)
        {
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception
    {
        Namespace c_args = parseArgs(args);
        if (c_args == null)
        {
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try
        {
        	client.go(c_args.getString("operation"), c_args.getString("filePath"), c_args.getString("downloadDir"));

        } finally
        {
            client.shutdown();
        }
    }

}
