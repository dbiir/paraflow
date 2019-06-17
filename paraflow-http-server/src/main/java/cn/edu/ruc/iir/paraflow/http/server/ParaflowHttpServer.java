package cn.edu.ruc.iir.paraflow.http.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * paraflow
 *
 * @author guodong
 */
@SpringBootApplication
@ComponentScan
public class ParaflowHttpServer
{
    public static void main(String[] args)
    {
        ParaflowHttpServer httpServer = new ParaflowHttpServer();
        httpServer.init();
        SpringApplication.run(ParaflowHttpServer.class, args);
    }

    private void init()
    {
        System.out.println("Paraflow http server is starting...");
    }
}
