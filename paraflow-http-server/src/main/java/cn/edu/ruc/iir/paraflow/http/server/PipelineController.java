package cn.edu.ruc.iir.paraflow.http.server;

import cn.edu.ruc.iir.paraflow.http.server.model.ClusterInfo;
import cn.edu.ruc.iir.paraflow.http.server.model.Pipeline;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * paraflow
 *
 * @author guodong
 */
@RestController
@EnableAutoConfiguration
public class PipelineController
{
    @CrossOrigin
    @GetMapping("/info")
    public ClusterInfo info()
    {
        return new ClusterInfo();
    }

    @RequestMapping("/pipeline/p0")
    public String pipeline()
    {
        Pipeline p0 = new Pipeline("p0");
        return p0.toString();
    }
}
