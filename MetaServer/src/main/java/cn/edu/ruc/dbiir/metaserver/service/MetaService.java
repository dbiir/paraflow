package cn.edu.ruc.dbiir.metaserver.service;

import cn.edu.ruc.dbiir.metaserver.proto.MetaGrpc;
import cn.edu.ruc.dbiir.metaserver.proto.MetaProto;
import io.grpc.stub.StreamObserver;

/**
 * ParaFlow
 *
 * @author guodong
 */
public class MetaService extends MetaGrpc.MetaImplBase
{
    @Override
    public void sayHi(MetaProto.Request request, StreamObserver<MetaProto.Response> responseStreamObserver)
    {
        MetaProto.Response response = MetaProto.Response.newBuilder().setStatus(MetaProto.Response.State.OK).build();
        responseStreamObserver.onNext(response);
        responseStreamObserver.onCompleted();
    }
}
