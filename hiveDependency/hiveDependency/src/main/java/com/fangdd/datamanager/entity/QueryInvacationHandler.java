package com.fangdd.datamanager.entity;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;

public class QueryInvacationHandler implements InvocationHandler{

	private InetSocketAddress addr;
	
	public QueryInvacationHandler(InetSocketAddress addr, Class<?> serviceClass) {
		super();
		this.addr = addr;
		this.serviceClass = serviceClass;
	}

	private Class<?> serviceClass;

	@Override
	public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
		System.out.println("向远程服务发送调用请求............");
		Socket socket = null;
		ObjectOutputStream output = null;
		ObjectInputStream input = null;
		Object resultObj = null;
		try {
			socket = new Socket();
			socket.connect(addr);
			output = new ObjectOutputStream(socket.getOutputStream());
			output.writeUTF(serviceClass.getName());
			output.writeUTF(method.getName());
			output.writeObject(method.getParameterTypes());
			output.writeObject(args);
			output.flush();
			input = new ObjectInputStream(socket.getInputStream());
			resultObj = input.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (output != null) {
					output.close();
				}
				if (input != null) {
					input.close();
				}
				if (socket != null) {
					socket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("远程服务调用完成............");
		return resultObj;
	}

}
