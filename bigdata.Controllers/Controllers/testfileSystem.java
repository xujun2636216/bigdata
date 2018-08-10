package Controllers;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class testfileSystem {
    private FileSystem fs;

    public void InitCof() {
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(conf);
        } catch (Exception e) {

        }
    }

    /**
     * hdfs的写操作
     */

    public void writefile() throws IOException {

        Path path = new Path("hdfs://s100:8020/User/how.txt");
        // 数据输出流
        FSDataOutputStream dos = fs.create(path);
        dos.write("hellword".getBytes());
        dos.close();
        System.out.println("over");
    }

    /**
     * hdfs的写操作 指定副本数
     */
    public void writefilereplication() throws IOException {

        Path path = new Path("hdfs://s100:8020/User/how.txt");
        // 数据输出流,指定副本数
        FSDataOutputStream dos = fs.create(path, (short) 2);
        dos.write("hellwords's".getBytes());
        dos.close();
        System.out.println("over");
    }

    /**
     * hdfs的读操作
     */
    public void readFile() throws IOException {
        Path path = new Path("hdfs://s100:8020/ubuntu/hello.txt");
        FSDataInputStream fis = fs.open(path);
        FileOutputStream fos = new FileOutputStream("d:/hell0.txt");
        IOUtils.copyBytes(fis, fos, 1024);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * hdfs的读操作,只读取第一块(128M)
     */
    public void readFileSeek128() throws IOException {
        Path path = new Path("hdfs://s100:8020/user/ubuntu/data/hadoop-2.7.5.tar.gz");
        FSDataInputStream fis = fs.open(path);
        FileOutputStream fos = new FileOutputStream("d:/hadoop-2.7.5.tar.gz.part-0");
        int count = 128 * 1024 * 1024;
        int len = 1;
        byte[] buf = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);

        }
        IOUtils.copyBytes(fis, fos, 1024);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        System.out.println("read over");
    }

    /**
     * hdfs的读操作,定义偏移量
     */
    public void readFileSeek() throws IOException {
        Path path = new Path("hdfs://s100:8020/user/ubuntu/data/hadoop-2.7.5.tar.gz");
        FSDataInputStream fis = fs.open(path);
        // 定义文件偏移量
        fis.seek(1024 * 1024 * 128);
        FileOutputStream fos = new FileOutputStream("d:/hadoop-2.7.5.tar.gz.part-1");
        IOUtils.copyBytes(fis, fos, 1024);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        System.out.println("read over");
    }


    /**
     * hdfs的操作,创建文件夹
     */
    public void mkdir() throws IOException {
        Path path = new Path("/user/myfolder");
        //创建权限
        //FsPermission perm=new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
        //创建目录指定权限
        boolean b = fs.mkdirs(path, FsPermission.getDirDefault());
        System.out.println(b);

    }

    /**
     * hdfs的操作,遍历目录
     */
    public void fileState() throws IOException {

        FileStatus fs0 = new FileStatus();
        Class clazz = FileStatus.class;
        Method[] ms = clazz.getDeclaredMethods();
        for (Method m : ms) {
            String mname = m.getName();
            Class[] ptype = m.getParameterTypes();
            if (mname.startsWith("get") && (ptype == null || ptype == null)) {
                if (!mname.equals("getSymlink")) {
                    Object ret = null;
                    try {
                        ret = m.invoke(fs0, null);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {

                        e.printStackTrace();
                    }
                    System.out.println(mname + "()=" + ret);
                }
            }
        }

    }


    /**
     * 遍历目录
     */
    public void reseviceHdfsfile() throws IOException {

        FileStatus root = fs.getFileStatus(new Path("/"));
        print(root);

    }

    private void print(FileStatus fs0) {

        try {
            Path p = fs0.getPath();
            //打印路径名
            System.out.println(p.toUri().getPath());
            if (fs0.isDirectory()) {
                //列处路径下的所有资源
                FileStatus[] fss = fs.listStatus(p);
                if (fss != null && fss.length > 0) {
                    for (FileStatus ff : fss) {
                        print(ff);
                    }
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 刪除文件或者文件夾
     */
    public void deleteFile() throws IOException {

        Path p = new Path("/user/hello.txt");
        fs.delete(p, true);
    }
}
