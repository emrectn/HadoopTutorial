
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;


public class MapReduceHdfs {
	private static Configuration configuration;
	private static URI uri;
	private static FileSystem fileSystem;
    private static FileSystem localSystem;
    private static String status;
	
	public static void main(String [] args) throws IOException, URISyntaxException{
		try {
			status = args[0].toString().toLowerCase();
			if (status == null || status.length() == 0){
				throw new ArrayIndexOutOfBoundsException("Null");
			}
			init();

			if (status.equals("getfilestatus")) {
				
				System.out.println("\ngetFileStatus");
				System.out.println("-----------------------");
				getFileStatus(args[1]);
			}
	
			else if (status.equals("mkdir")) {
				System.out.println("\nmkdir");
				System.out.println("-----------------------");
				mkdir(args[1]);
			}
			
			else if (status.equals("createfile")) {
				System.out.println("\ncreateFile");
				System.out.println("-----------------------");
				createFile(args[1]);
			}
	
			else if (status.equals("readfile")) {
				System.out.println("\nreadFile");
				System.out.println("-----------------------");
				readFile(args[1]);
			}        
			
			else if (status.equals("deletefile")) {
				System.out.println("\ndeleteFile");
				System.out.println("-----------------------");
				deleteFile(args[1], true);
			}
	
			else if (status.equals("copyfile")) {
				System.out.println("copyFile");
				System.out.println("-----------------------");
				// copyFile("/root/ee/oldumu.txt", "/user/root/input/merge.txt");
				copyFile(args[1], args[2]);
			}
	
			else {
				throw new ArrayIndexOutOfBoundsException("Gecersiz");
			}
	
			
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("\n-----------------------------------------");
			System.out.println("Eksik veya Gecersiz Parametre girdiniz !");
			System.out.println("-----------------------------------------");
			System.out.println(" Kullanilabilir parametreler : ");
			System.out.println("   - getfilestatus {filename}");
			System.out.println("   - mkdir {filename}");
			System.out.println("   - createfile {filename}");
			System.out.println("   - readfile {filename}");
			System.out.println("   - deletefile {filename}");
			System.out.println("   - copyfile {source} {destination}");
			System.out.println("   - hadoop jar hdfspr.jar MapReduceHdfs {arg1} {arg2}");
			System.out.println("-----------------------------------------");

		}
    }

	/*
	 * initialize the Haddop Cluster configuration
	 */
	public static void init() throws URISyntaxException, IOException{
		configuration = new Configuration();
		uri = new URI("hdfs://hadoop-master:9000");
		fileSystem = (DistributedFileSystem) FileSystem.get(uri, configuration);
		localSystem = FileSystem.getLocal(configuration);
		
	}
	
	public static void createFile(String fileName) throws IllegalArgumentException, IOException{
		FSDataOutputStream outputStream = fileSystem.create(new Path(fileName));
		outputStream.close();
		
	}
	
	public static void readFile(String fileName) throws IllegalArgumentException, IOException{
		FSDataInputStream inputStream = fileSystem.open(new Path(fileName));
		IOUtils.copyBytes(inputStream, System.out, configuration);
		IOUtils.closeStream(inputStream);
	}
		
	public static void mkdir(String dirName) throws IOException{
		Path dir = new Path(dirName);
		boolean flag = fileSystem.mkdirs(dir);
		if (flag) {
			System.out.println("success to mkdir " + dirName);
		}else {
			System.out.println("fialed to mkdir " + dirName);
		}
		
	}

	public static void deleteFile(String dirName,boolean recursive) throws IllegalArgumentException, IOException {
		boolean flag = fileSystem.delete(new Path(dirName), recursive);
		System.out.println("flag=" + flag);
	
	}

	public static void getFileStatus(String filePath) throws IOException {
		Path path = new Path(filePath);
		FileStatus fileStatus = fileSystem.getFileStatus(path);
		System.out.println("path= " +fileStatus.getPath() );
		System.out.println("IsDirectory=" + fileStatus.isDirectory());
		System.out.println("length:" + fileStatus.getLen());
		System.out.println("replication=" + fileStatus.getReplication());
	}

	public static void copyFile(String dirName,String hdfsFileName) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		FileStatus[] listStatus = localSystem.listStatus(new Path(dirName));
		Path hdfsfile = new Path(hdfsFileName);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsfile);
		for (FileStatus fileStatus : listStatus) {
			Path path = fileStatus.getPath();
			FSDataInputStream fsDataInputStream = localSystem.open(path);
			byte[] buffer = new byte[256];
			int len ;
			while((len = fsDataInputStream.read(buffer)) > 0){
				fsDataOutputStream.write(buffer, 0, len);
			}
			fsDataInputStream.close();
		}
		fsDataOutputStream.close();
		
		
	}
	
}

