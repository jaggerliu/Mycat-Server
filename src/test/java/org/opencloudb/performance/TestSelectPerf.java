package org.opencloudb.performance;

import java.io.PrintStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class TestSelectPerf
{
  private AtomicLong finshiedCount = new AtomicLong();
  private AtomicLong failedCount = new AtomicLong();
  private long start;
  private ExecutorService executor = null;
  
  public SimpleConPool getConPool(String url, String user, String password, int threadCount)
    throws SQLException, ClassNotFoundException
  {
    Class.forName("com.mysql.jdbc.Driver");
    SimpleConPool conPool = new SimpleConPool(url, user, password, threadCount);
    return conPool;
  }
  
  private void doTest(String url, String user, String password, int threadCount, long minId, long maxId, int executetimes, boolean outmidle)
  {
    this.executor = Executors.newFixedThreadPool(threadCount);
    SimpleConPool conPool = null;
    try
    {
      conPool = getConPool(url, user, password, threadCount);
    }
    catch (Exception e1)
    {
      e1.printStackTrace();
      return;
    }
    this.start = System.currentTimeMillis();
    for (int i = 0; i < threadCount; i++) {
      try
      {
        TravelRecordSelectJob job = new TravelRecordSelectJob(conPool, minId, maxId, executetimes, this.finshiedCount, this.failedCount);
        
        this.executor.execute(job);
      }
      catch (Exception e)
      {
        System.out.println("failed create thread " + i + " err " + e.toString());
      }
    }
    System.out.println("success create thread count: " + threadCount);
    System.out.println("all thread started,waiting finsh...");
    try
    {
      report();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    Class.forName("com.mysql.jdbc.Driver");
    if (args.length < 5)
    {
      System.out.println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  [executetimes] [minId-maxId] [repeat]");
      return;
    }
    int threadCount = 0;
    String url = args[0];
    String user = args[1];
    String password = args[2];
    threadCount = Integer.parseInt(args[3]);
    int executetimes = Integer.parseInt(args[4]);
    long minId = Integer.parseInt(args[5].split("-")[0]);
    long maxId = Integer.parseInt(args[5].split("-")[1]);
    System.out.println("concerent threads:" + threadCount);
    System.out.println("execute sql times:" + executetimes);
    System.out.println("maxId:" + maxId);
    int repeate = 1;
    if (args.length > 6)
    {
      repeate = Integer.parseInt(args[6]);
      System.out.println("repeat test times:" + repeate);
    }
    try
    {
      new TestSelectPerf().doTest(url, user, password, threadCount, minId, maxId, executetimes, repeate < 2);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public void report()
    throws InterruptedException
  {
    this.executor.shutdown();
    
    SimpleDateFormat df = new SimpleDateFormat("dd HH:mm:ss");
    while (!this.executor.isTerminated())
    {
      long sucess = this.finshiedCount.get() - this.failedCount.get();
      System.out.println(df.format(new Date()) + " finished :" + this.finshiedCount
        .get() + " failed:" + this.failedCount
        .get() + " speed:" + sucess * 1000.0D / (
        System.currentTimeMillis() - this.start));
      Thread.sleep(1000L);
    }
    long usedTime = (System.currentTimeMillis() - this.start) / 1000L;
    System.out.println("finishend:" + this.finshiedCount.get() + " failed:" + this.failedCount
      .get());
    long sucess = this.finshiedCount.get() - this.failedCount.get();
    System.out.println("used time total:" + usedTime + "seconds");
    System.out.println("qps:" + sucess / (usedTime + 0.1D));
  }
}
