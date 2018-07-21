package cn.edu.ruc.iir.paraflow.http.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * paraflow
 *
 * @author guodong
 */
@RestController
@SpringBootApplication
public class ParaflowHttpServer
{
    @RequestMapping("/index")
    public String index()
    {
        return "Hello world!";
    }

    public static void main(String[] args)
    {
        SpringApplication.run(ParaflowHttpServer.class, args);
    }
}
