package org.opencloudb.performance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TravelRecordSelectJob
  implements Runnable
{
  private final SimpleConPool conPool;
  private final long minId;
  private final long maxId;
  private final int executeTimes;
  private final AtomicLong finshiedCount;
  private final AtomicLong failedCount;
  Random random = new Random();
  
  public TravelRecordSelectJob(SimpleConPool conPool, long minId, long maxId, int executeTimes, AtomicLong finshiedCount, AtomicLong failedCount)
  {
    this.conPool = conPool;
    this.minId = minId;
    this.maxId = maxId;
    this.executeTimes = executeTimes;
    this.finshiedCount = finshiedCount;
    this.failedCount = failedCount;
  }
  
  private void select()
  {
    ResultSet rs = null;
    Connection conn = null;
    PreparedStatement ps = null;
    try
    {
      conn = this.conPool.getConnection();
      
      String sql = "select * from  travelrecord  where id=" + (Math.abs(this.random.nextLong()) % (this.maxId - this.minId) + this.minId);
      ps = conn.prepareStatement(sql);
      rs = ps.executeQuery(sql);
      





      this.finshiedCount.incrementAndGet(); return;
    }
    catch (Exception e)
    {
      this.failedCount.incrementAndGet();
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (ps != null) {
          ps.close();
        }
        if (rs != null) {
          rs.close();
        }
        this.conPool.returnCon(conn);
      }
      catch (SQLException e) {}
    }
  }
  
  public void run()
  {
    for (int i = 0; i < this.executeTimes; i++) {
      select();
    }
  }
  
  public static void main(String[] args)
  {
    Random r = new Random();
    for (int i = 0; i < 10; i++)
    {
      int f = r.nextInt(10000) + 80000;
      System.out.println(f);
    }
  }
}
