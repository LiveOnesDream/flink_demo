package monitor;

import java.io.*;
import java.util.Properties;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

/**
 * @author zhangpeng.sun
 * @ClassName: TailLog2File
 * @Description TODO
 * @date 2021/8/5 16:09
 */
public class TailLog2File {
    private static Connection conn;
    private static int threadNum = 0;
    private static String cmd = "echo no commond";

    public static void main(String[] args) {
        Properties properties = new Properties();
        InputStream in;
        try {
            in = new BufferedInputStream(new FileInputStream(new File("/var/log/cloudera-scm-server/cloudera-scm-server.log")));
            properties.load(in);
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //远程连接linux服务器
        String ip = properties.getProperty("121.40.28.59");
        String usr = properties.getProperty("root");
        String psword = properties.getProperty("supersg123!");
        cmd = properties.getProperty("shell");
        //创建远程连接，默认连接端口为22，如果不使用默认，可以使用方法
        //new Connection(ip, port)创建对象
        conn = new Connection(ip,22);
        try {
            //连接远程服务器
            conn.connect();
            //使用用户名和密码登录
            conn.authenticateWithPassword(usr, psword);
        } catch (
                IOException e) {
            System.err.printf("用户%s密码%s登录服务器%s失败！", usr, psword, ip);
            e.printStackTrace();
        }
        //创建线程,执行shell命令，获取实时数据流,写入
        threadNum = 1;
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Session session = conn.openSession();
                    session.execCommand(cmd);
                    InputStream out = new StreamGobbler(session.getStdout());
                    BufferedReader outBufferedReader = new BufferedReader(new InputStreamReader(out));
                    while (true) {
                        String line = outBufferedReader.readLine();
                        if (line == null) {
                            threadNum = 0;
                            outBufferedReader.close();
                            session.close();
                            conn.close();
                            break;
                        }
                        System.out.println(line);
                    }
                } catch (IOException e) {
                    System.out.println("open session fail");
                    e.printStackTrace();
                }
            }
        }).start();
        while (threadNum > 0) {
            Thread.yield();
        }
    }
}


