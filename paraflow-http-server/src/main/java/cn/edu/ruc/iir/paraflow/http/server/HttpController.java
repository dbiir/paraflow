package cn.edu.ruc.iir.paraflow.http.server;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * paraflow
 *
 * @author guodong
 */
@Controller
public class HttpController
{
    @RequestMapping("/")
    public String index()
    {
        return "index.html";
    }
}
