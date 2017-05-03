package com.fangdd.datamanager.RPC;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fangdd.datamanager.entity.RequestEntity;

/**
 * RPC服务端服务发布者代码实现
 * 
 * 职责：运行在RPC服务端，负责将本地服务发布成远程服务，供其他消费者使用,具体职责如下：
 * ①作为服务端，监听客户端的TCP连接，接收到新的客户端连接之后，将其封装成Task，由线程池执行
 * ②将客户端发送的码流发序列化成对象，反射调用服务实现者，获取执行结果 ③将执行结果对象反序列化，通过Socket发送给客户端
 * ④远程服务调用完成之后，释放Socket等连接资源，防止句柄泄漏
 * 
 * @author sh-yanggang
 *
 */
public class RpcExporter {

	private static Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	public static void export(String hostname, int port) throws Exception {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress(hostname, port));
			System.out.println("服务已启动，准备接受消费者调用请求..........");
			while (true) {// 持续接受消费者调用
				executor.execute(new ExporterTask(server.accept()));// 阻塞到捕捉到一个来自client端的请求为止
			}
		} catch (IOException e) {
			throw e;
		}
	}

	private static class ExporterTask implements Runnable {

		private Socket client = null;

		public ExporterTask(Socket client) {
			super();
			this.client = client;
		}

		@Override
		public void run() {
			System.out.println("开始执行调用.....");
			ObjectInputStream input = null;
			ObjectOutputStream output = null;
			try {
				System.out.println("开始获取调用参数流......");
				input = new ObjectInputStream(client.getInputStream());
				String interfaceName = input.readUTF();
				System.out.println("服务端获取到得接口名称是："+interfaceName);
				Class<?> service = Class.forName(interfaceName);
				String methodName = input.readUTF();
				System.out.println("服务端获取到得方法名称是："+methodName);
				Class<?>[] parameterTypes = (Class<?>[]) input.readObject();
				Object[] arguments = (Object[]) input.readObject();
				Method method = service.getDeclaredMethod(methodName, parameterTypes);
				Object result = method.invoke(service.newInstance(), arguments);
				output = new ObjectOutputStream(client.getOutputStream());
				output.writeObject(result);
				output.flush();
				System.out.println("调用结束并返回结果.....");
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
					
					if (client != null) {
						client.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}
}
