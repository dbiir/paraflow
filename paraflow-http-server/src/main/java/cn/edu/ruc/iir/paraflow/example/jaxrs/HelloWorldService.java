package cn.edu.ruc.iir.paraflow.example.jaxrs;

/**
 * Created by tamao on 10/11/15.
 */

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

@Path("/hello")
public class HelloWorldService
{
    @GET
    @Path("/{param}")
    public Response getMsg(@PathParam("param") String msg)
    {
        String output = "Hello say : " + msg;
        return Response.status(200).entity(output).build();
    }
}
