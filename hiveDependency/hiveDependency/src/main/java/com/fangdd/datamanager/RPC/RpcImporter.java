package com.fangdd.datamanager.RPC;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

import com.fangdd.datamanager.entity.QueryInvacationHandler;


/**
 * RPC客户端本地服务代理,职责如下：
 * ①将本地的接口调用转换成JDK的动态代理，在动态代理中实现接口
 * ②创建Socket客户端，根据指定地址连接远程服务提供者
 * ③将远程服务调用所需的接口类、方法名、参数列表等编码后发送给服务提供者
 * ④同步阻塞等待服务端的返回应答，获取之后返回
 * @author sh-yanggang
 *
 * @param <S>
 */
public class RpcImporter<S> {
	
	@SuppressWarnings("all")
    public S importer(final Class<?> serviceClass,final InetSocketAddress addr){
    	return (S)Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class<?>[]{
    		serviceClass.getInterfaces()[0]
    	},new QueryInvacationHandler(addr, serviceClass));
    }
}
