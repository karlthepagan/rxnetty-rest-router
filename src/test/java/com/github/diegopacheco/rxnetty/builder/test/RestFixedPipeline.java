package com.github.diegopacheco.rxnetty.builder.test;

import com.google.auto.value.AutoValue;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.net.URI;
import java.net.URISyntaxException;

@Path("/pipeline")
public class RestFixedPipeline {
    @GET
    @Path("now")
    public String maskedOnNext(){
        return "OK";
    }

    @GET
    @Path("path/{a}/{b}")
    public String maskedOnNextWithArgs(@PathParam("a") String a, @PathParam("b") String b){
        return "Path: " +  a + " - " + b;
    }

    @GET
    @Path("subscriber/{a}/{b}")
    public Subscriber<? super ABRequest> explicitOperator(Subscriber<? super HttpServerResponse<ByteBuf>> resps) {
        // onSubscribe - setup route resources

        return new Subscriber<ABRequest>() {
            @Override
            public void onCompleted() {
                // route safe shutdown
                resps.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                // route will be shut down and restarted?
                // yes! any downstream service hooks should be rebuilt
                // database/resource retries might fire up
                // in-flight requests should be re-issued
                resps.onError(e);
            }

            @Override
            public void onNext(ABRequest req) {
                HttpServerResponse<ByteBuf> resp = req.response();
                resp.setStatus(HttpResponseStatus.OK);
                // write to resp
                resps.onNext(resp); // flush/uncork/release stream/etc
            }
        };
    }

    @GET
    @Path("liftedSubscriber/{a}/{b}")
    public Observable<HttpServerResponse<ByteBuf>> liftedOperator(Observable<ABRequest> request) {
        return request.lift(s -> explicitOperator(s));
    }

    @GET
    @Path("observable/{a}/{b}")
    public Observable<HttpServerResponse<ByteBuf>> explicitObservable(Observable<ABRequest> request) {
        return request
                .map( r -> r.response() )
                .doOnNext( r -> r.setStatus(HttpResponseStatus.OK) );
    }

    private static final String REST_ROUTE = "observable/{a}/{b}";

    @GET
    @Path( REST_ROUTE )
    public Observable<HttpServerResponse<ByteBuf>> observableWithRouteRetry( Observable<ABRequest> request ) {
        return request
                .groupBy( ABRequest::a )
                .map( group -> group.zipWith(
                            getDownstream( group.getKey() ),
                            (shardRequest, uri) -> proxyDownstream( uri, shardRequest.response() )
                        )
                        // on failure, reload per-shard route
                        .retryWhen( getRetryStrategy( REST_ROUTE, group.getKey() ) )
                )
                .flatMap( Observable::asObservable )
                .onErrorResumeNext( getErrorStrategy( REST_ROUTE ) );
    }

    @AutoValue
    static abstract class ABRequest {
        @PathParam("a")
        abstract String a();
        @PathParam("b")
        abstract String b();
        abstract HttpServerRequest<ByteBuf> request();
        abstract HttpServerResponse<ByteBuf> response();
    }

    private Observable<URI> getDownstream(String shard) {
        return Observable.create( s -> {
            try {
                s.onNext(new URI("http://google.com"));
                s.onCompleted();
            } catch (Exception e) {
                s.onError(e);
            }
        });
    }

    private Func1<? super Observable<? extends Throwable>, ? extends Observable<?>> getRetryStrategy(String groupName, String shard) {
        return null;
    }

    private HttpServerResponse<ByteBuf> proxyDownstream(URI uri, HttpServerResponse<ByteBuf> response) {
        return response;
    }

    private Func1<Throwable, Observable<? extends HttpServerResponse<ByteBuf>>> getErrorStrategy(String myRouteGroup) {
        return null;
    }
}
