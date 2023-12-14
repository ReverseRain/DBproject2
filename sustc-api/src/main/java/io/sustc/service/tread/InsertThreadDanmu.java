package io.sustc.service.tread;

import io.sustc.dto.DanmuRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class InsertThreadDanmu implements Runnable{
    private Connection connection;
    private PreparedStatement stmt;
    private PreparedStatement stmtDanmu;
    private List<DanmuRecord> dataList;
    private long start;

    public InsertThreadDanmu(Connection connection, List<DanmuRecord> dataList,long start) {
        this.connection = connection;
        try {
            stmt = connection.prepareStatement("insert into danmu(bv,mid,time,content,postTime)" + "values(?,?,?,?,?)");
            stmtDanmu=connection.prepareStatement("insert into danmuLikeBy(danmuID,likeMid) values(?,?)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.dataList = dataList;
        this.start=start;
    }

    @Override
    public void run() {
        long num=0;
        for (int i = 0; i < dataList.size(); i++) {

            try {
                loadDataDanmu(dataList.get(i).getBv(),dataList.get(i).getMid(),
                        dataList.get(i).getTime(),dataList.get(i).getContent(),
                        dataList.get(i).getPostTime());
                for (int j = 0; j < dataList.get(i).getLikedBy().length; j++) {
                    loadDataLikeBy((int)num+1,dataList.get(i).getLikedBy()[j]);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            num++;
//            System.out.println(num+"弹幕");
            if (num%500==0){
                try {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    stmtDanmu.executeBatch();
                    stmtDanmu.clearBatch();
                    mycommit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            stmt.executeBatch();
            stmt.clearBatch();
            stmtDanmu.executeBatch();
            stmtDanmu.clearBatch();
            mycommit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long cur=System.currentTimeMillis()-start;
        System.out.println("时间"+cur);
    }
    private void loadDataDanmu(String BV, long mid, float time, String content, Timestamp postTime) throws SQLException {
        if (connection != null) {
            stmt.setString(1, BV);
            stmt.setLong(2, mid);
            stmt.setFloat(3, time);
            stmt.setString(4, content);
            stmt.setTimestamp(5,postTime);
            stmt.addBatch();
        }
    }
    private void loadDataLikeBy(int danmuID,long likeMid) throws SQLException {
        if (connection != null) {
            stmtDanmu.setInt(1, danmuID);
            stmtDanmu.setLong(2,likeMid);
            stmtDanmu.addBatch();
        }
    }
    private synchronized void mycommit(){
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

