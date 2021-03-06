package com.github.diegopacheco.rxnetty.router;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.Observable;

/**
 * JerseyRouter is a simple router for @Path REST classes into RxNetty.
 * 
 * @author diegopacheco
 *
 */
@SuppressWarnings({"rawtypes"})
public class JerseyRouter {
	
	private final static Logger logger = Logger.getLogger(JerseyRouter.class);

	private Injector injector;
	private Map<String,Method> handlers = new HashMap<>();
	private AnnotationScanner scanner;
	
	public JerseyRouter(String basePackage,Module... modules){
		logger.info("Scanning base packages: " + basePackage);
		
		this.injector = Guice.createInjector(modules);
		scanner = new AnnotationScanner(basePackage);
		handlers = scanner.getHandlers();
	}
	
	public Observable handle(HttpServerRequest<ByteBuf> req, HttpServerResponse<ByteBuf> resp) {
		logger.info("Processing URI: " + req.getUri());
		
		Object[] invokeArgs = null;
		Method m = handlers.get(req.getUri());
		if(m==null){
			if ("/favicon.ico".equals(req.getUri())) return Observable.empty();
			
			List<PatternMethod> pms = scanner.getPatternsMethods();
			String requestValue = req.getUri();
			if(!requestValue.endsWith("/"))
				requestValue += "/";
			
			Boolean found = false;
			
			for(PatternMethod pp : pms){
				if (pp.getPattern().matcher(requestValue).find()){
					m = pp.getMethod();
					found = true;
					
					String extraPath = req.getUri().replace(pp.getBasePath(), "");
					StringTokenizer st = new StringTokenizer(extraPath, "/");
					List<Object> methodArgs = new ArrayList<>();
					
					while(st.hasMoreTokens()){
						methodArgs.add(st.nextToken());
					}
					invokeArgs = methodArgs.toArray();
					break;
				}
			}
			if (!found)
				throw new NoHandlerFoundException("No Handler found for URI: " + req.getUri());
		}
		
		Object insatance = injector.getInstance(m.getDeclaringClass());
		Object result = null;
		try {
			AnnotatedType[] args =  m.getAnnotatedParameterTypes();
			if(args.length==1){
				if ("io.reactivex.netty.protocol.http.server.HttpServerRequest<io.netty.buffer.ByteBuf>".equals(args[0].getType().getTypeName()) ){
					result = m.invoke(insatance,req);
				}else if("io.reactivex.netty.protocol.http.server.HttpServerResponse<io.netty.buffer.ByteBuf>".equals(args[0].getType().getTypeName()) ){
					result = m.invoke(insatance,resp);
				}
			} else if(args.length==2){
				if ("io.reactivex.netty.protocol.http.server.HttpServerRequest<io.netty.buffer.ByteBuf>".equals(args[0].getType().getTypeName()) && 
				    "io.reactivex.netty.protocol.http.server.HttpServerResponse<io.netty.buffer.ByteBuf>".equals(args[1].getType().getTypeName())){
					result = m.invoke(insatance,req,resp);
				} else if (invokeArgs!=null)
					result = m.invoke(insatance,invokeArgs);
			} else{
					result = m.invoke(insatance);
			}
			if(!(result instanceof Observable)){
				result = Observable.just(result);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return (Observable)result;
	}
	
}