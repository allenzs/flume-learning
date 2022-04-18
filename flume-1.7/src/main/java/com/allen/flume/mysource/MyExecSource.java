package com.allen.flume.mysource;


import com.allen.flume.util.RedisUtil;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 *
 */
public class MyExecSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(org.apache.flume.source.ExecSource.class);

	private String shell;
	private String command;
	private SourceCounter sourceCounter;
	private ExecutorService executor;
	private Future<?> runnerFuture;
	private long restartThrottle;
	private boolean restart;
	private boolean logStderr;
	private Integer bufferCount;
	private long batchTimeout;
	private ExecRunnable runner;
	private Charset charset;

	@Override
	public void start() {
		logger.info("Exec source starting with command:{}", command);

		executor = Executors.newSingleThreadExecutor();

		runner = new ExecRunnable(shell, command, getChannelProcessor(),
																			sourceCounter, restart, restartThrottle,
																			logStderr, bufferCount, batchTimeout,
																			charset);
		runnerFuture = executor.submit(runner);

		sourceCounter.start();
		super.start();

		//启动防丢失数据线程
		Thread t = new Thread(){
			public void run() {
				System.out.println("Deamon starting .......");
				while(true){
					File dir = new File("/usr/local/openresty/nginx/logs") ;
					File[] files = dir.listFiles();
					for(File f : files){
						String fname = f.getName();
						//没有处理完成的文件
						if(fname.startsWith("access.log.") && !fname.endsWith(".completed")){
							processLogFile(f);
						}
					}
				}
			}

			private void processLogFile(File f) {
				try {
					BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f))) ;
					String line = null ;
					while((line = reader.readLine()) != null){
						if(!line.trim().endsWith("")){
							String key = line.substring(0,line.indexOf("{")) ;
							if(!RedisUtil.existsInRedis(key)){
								Event e = EventBuilder.withBody(line.getBytes());
								MyExecSource.this.getChannelProcessor().processEvent(e);
							}
						}
					}
					//处理完成
					//f.renameTo(f.getAbsolutePath()) ;
					f.renameTo(new File(f.getAbsolutePath() + ".completed")) ;
					Thread.sleep(1000);
				} catch (Exception e) {
					System.out.println(f.getAbsolutePath() + "处理失败！");
					return  ;
				}
			}
		};
		t.setDaemon(true) ;
		t.start();
		logger.debug("Exec source started");
	}

	@Override
	public void stop() {
		logger.info("Stopping exec source with command:{}", command);
		if (runner != null) {
			runner.setRestart(false);
			runner.kill();
		}

		if (runnerFuture != null) {
			logger.debug("Stopping exec runner");
			runnerFuture.cancel(true);
			logger.debug("Exec runner stopped");
		}
		executor.shutdown();

		while (!executor.isTerminated()) {
			logger.debug("Waiting for exec executor service to stop");
			try {
				executor.awaitTermination(500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.debug("Interrupted while waiting for exec executor service " + "to stop. Just exiting.");
				Thread.currentThread().interrupt();
			}
		}

		sourceCounter.stop();
		super.stop();

		logger.debug("Exec source with command:{} stopped. Metrics:{}", command, sourceCounter);
	}

	public void configure(Context context) {
		command = context.getString("command");

		Preconditions.checkState(command != null, "The parameter command must be specified");

		restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
				ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

		restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
				ExecSourceConfigurationConstants.DEFAULT_RESTART);

		logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
				ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

		bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
				ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

		batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
				ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

		charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
				ExecSourceConfigurationConstants.DEFAULT_CHARSET));

		shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	private static class ExecRunnable implements Runnable {

		public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor, SourceCounter sourceCounter, boolean restart, long restartThrottle, boolean logStderr, int bufferCount, long batchTimeout, Charset charset) {
			this.command = command;
			this.channelProcessor = channelProcessor;
			this.sourceCounter = sourceCounter;
			this.restartThrottle = restartThrottle;
			this.bufferCount = bufferCount;
			this.batchTimeout = batchTimeout;
			this.restart = restart;
			this.logStderr = logStderr;
			this.charset = charset;
			this.shell = shell;
		}

		private final String shell;
		private final String command;
		private final ChannelProcessor channelProcessor;
		private final SourceCounter sourceCounter;
		private volatile boolean restart;
		private final long restartThrottle;
		private final int bufferCount;
		private long batchTimeout;
		private final boolean logStderr;
		private final Charset charset;
		private Process process = null;
		private SystemClock systemClock = new SystemClock();
		private Long lastPushToChannel = systemClock.currentTimeMillis();
		ScheduledExecutorService timedFlushService;
		ScheduledFuture<?> future;

		public void run() {
			do {
				String exitCode = "unknown";
				BufferedReader reader = null;
				String line = null;
				final List<Event> eventList = new ArrayList<Event>();

				timedFlushService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(
						"timedFlushExecService" + Thread.currentThread().getId() + "-%d").build());
				try {
					if (shell != null) {
						String[] commandArgs = formulateShellCommand(shell, command);
						process = Runtime.getRuntime().exec(commandArgs);
					} else {
						String[] commandArgs = command.split("\\s+");
						process = new ProcessBuilder(commandArgs).start();
					}
					reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));

					// StderrLogger dies as soon as the input stream is invalid
					StderrReader stderrReader = new StderrReader(new BufferedReader(new InputStreamReader(process.getErrorStream(),
																																													   charset)),
																																			  logStderr);
					stderrReader.setName("StderrReader-[" + command + "]");
					stderrReader.setDaemon(true);
					stderrReader.start();

					future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
						public void run() {
							try {
								synchronized (eventList) {
									if (!eventList.isEmpty() && timeout()) {
										flushEventBatch(eventList);
									}
								}
							} catch (Exception e) {
								logger.error("Exception occured when processing event batch", e);
								if (e instanceof InterruptedException) {
									Thread.currentThread().interrupt();
								}
							}
						}
					}, batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

					while ((line = reader.readLine()) != null) {
						synchronized (eventList) {
							sourceCounter.incrementEventReceivedCount();
							eventList.add(EventBuilder.withBody(line.getBytes(charset)));
							if (eventList.size() >= bufferCount || timeout()) {
								flushEventBatch(eventList);
							}
						}
					}

					synchronized (eventList) {
						if (!eventList.isEmpty()) {
							flushEventBatch(eventList);
						}
					}
				} catch (Exception e) {
					logger.error("Failed while running command: " + command, e);
					if (e instanceof InterruptedException) {
						Thread.currentThread().interrupt();
					}
				} finally {
					if (reader != null) {
						try {
							reader.close();
						} catch (IOException ex) {
							logger.error("Failed to close reader for exec source", ex);
						}
					}
					exitCode = String.valueOf(kill());
				}
				if (restart) {
					logger.info("Restarting in {}ms, exit code {}", restartThrottle, exitCode);
					try {
						Thread.sleep(restartThrottle);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				} else {
					logger.info("Command [" + command + "] exited with " + exitCode);
				}
			} while (restart);
		}

		private void flushEventBatch(List<Event> eventList) {
			/**
			 *预处理事件集合
			 */
			preProcessEventList(eventList);
			channelProcessor.processEventBatch(eventList);
			sourceCounter.addToEventAcceptedCount(eventList.size());
			eventList.clear();
			lastPushToChannel = systemClock.currentTimeMillis();
		}

		/**
		 * 预处理事件集合
		 */
		private void preProcessEventList(List<Event> eventList) {

		}

		private boolean timeout() {
			return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
		}

		private static String[] formulateShellCommand(String shell, String command) {
			String[] shellArgs = shell.split("\\s+");
			String[] result = new String[shellArgs.length + 1];
			System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
			result[shellArgs.length] = command;
			return result;
		}

		public int kill() {
			if (process != null) {
				synchronized (process) {
					process.destroy();

					try {
						int exitValue = process.waitFor();

						// Stop the Thread that flushes periodically
						if (future != null) {
							future.cancel(true);
						}

						if (timedFlushService != null) {
							timedFlushService.shutdown();
							while (!timedFlushService.isTerminated()) {
								try {
									timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
								} catch (InterruptedException e) {
									logger.debug("Interrupted while waiting for exec executor service " + "to stop. Just exiting.");
									Thread.currentThread().interrupt();
								}
							}
						}
						return exitValue;
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				return Integer.MIN_VALUE;
			}
			return Integer.MIN_VALUE / 2;
		}

		public void setRestart(boolean restart) {
			this.restart = restart;
		}
	}

	private static class StderrReader extends Thread {
		private BufferedReader input;
		private boolean logStderr;

		protected StderrReader(BufferedReader input, boolean logStderr) {
			this.input = input;
			this.logStderr = logStderr;
		}

		@Override
		public void run() {
			try {
				int i = 0;
				String line = null;
				while ((line = input.readLine()) != null) {
					if (logStderr) {
						// There is no need to read 'line' with a charset
						// as we do not to propagate it.
						// It is in UTF-16 and would be printed in UTF-8 format.
						logger.info("StderrLogger[{}] = '{}'", ++i, line);
					}
				}
			} catch (IOException e) {
				logger.info("StderrLogger exiting", e);
			} finally {
				try {
					if (input != null) {
						input.close();
					}
				} catch (IOException ex) {
					logger.error("Failed to close stderr reader for exec source", ex);
				}
			}
		}
	}
}