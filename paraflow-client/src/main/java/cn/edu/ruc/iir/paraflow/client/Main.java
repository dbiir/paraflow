package cn.edu.ruc.iir.paraflow.client;

import cn.edu.ruc.iir.paraflow.commons.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.paraflow.client
 * @ClassName: Main
 * @Description: reference: https://blog.csdn.net/m0_38086372/article/details/77979345
 * @author: tao
 * @date: Create in 2019-10-13 11:45
 **/
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: input function");
            System.out.println("Usage: clear *.sh period");
            System.exit(-1);
        }
        System.out.println(Arrays.toString(args));
        String func = args[0];
        if (func.equals("clear")) {
            String path;
            int time = 10;
            // 有参数
            if (args.length == 2) {
                path = args[1];
            } else if (args.length == 3) {
                path = args[1];
                time = Integer.parseInt(args[2]);
            } else {
                path = "/home/iir/opt/paraflow-1.0-alpha1/sbin/logclean.sh";
            }

            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    System.out.println("now : " + DateUtil.formatTime(new Date()));
                    runShell(path);
                }
            };
            Timer timer = new Timer();
            long delay = 0;
            long period = 1000 * time;
            timer.scheduleAtFixedRate(task, delay, period);
        } else if (func.equals("")) {

        } else {

        }

    }

    public static void runShell(String path) {
        try {
            String getX = "chmod a+x " + path;
            // 给予执行权限
            Process process = Runtime.getRuntime().exec(getX);
            process.waitFor();
            // 执行脚本
            process = Runtime.getRuntime().exec("bash " + path);
            process.waitFor();
            // 获取执行结果
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
            // 输出执行结果
            System.out.println(stringBuilder.toString());
            logger.info("run shell:" + stringBuilder.toString() + "\n");
            logger.info("Shell run time:" + DateUtil.formatTime(new Date()));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
