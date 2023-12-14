package io.sustc.service.tread;

import io.sustc.dto.VideoRecord;

import java.sql.*;
import java.util.List;

public class InsertThreadVideo implements Runnable {
    private Connection connection;
    private PreparedStatement stmt, stmtLike, stmtCoin, stmtFar, stmtView;
    private List<VideoRecord> dataList;
    private long start;
    public InsertThreadVideo(Connection connection,List<VideoRecord> dataList,long start) {
        this.connection = connection;
        try {
            //sql change
            stmt = connection.prepareStatement("insert into videos(BV,title,ownerMid,ownerName,commitTime,reviewTime," +
                    "publicTime,duration,description,reviewerMid)" + "values(?,?,?,?,?,?,?,?,?,?)");
            stmtLike = connection.prepareStatement("insert into like_(BV,mid)" + "values(?,?)");
            stmtCoin = connection.prepareStatement("insert into coin(BV,mid)" + "values(?,?)");
            stmtFar = connection.prepareStatement("insert into favorite(BV,mid)" + "values(?,?)");
            stmtView = connection.prepareStatement("insert into view(BV,mid,watchTime)" + "values(?,?,?)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.dataList = dataList;
        this.start=start;
    }



    @Override
    public void run() {
        long num = 0;
        for (int j = 0; j < dataList.size(); j++) {
            try {
                loadDataVideo(dataList.get(j).getBv(), dataList.get(j).getTitle(),dataList.get(j).getOwnerMid(),
                        dataList.get(j).getOwnerName(), dataList.get(j).getCommitTime(), dataList.get(j).getReviewTime(),
                        dataList.get(j).getPublicTime(),dataList.get(j).getDuration(), dataList.get(j).getDescription(),dataList.get(j).getReviewer());
                for (int i = 0; i < dataList.get(i).getLike().length; i++) {
                    loadDataLike(dataList.get(j).getBv(),dataList.get(i).getLike()[i]);
                }
                for (int i = 0; i < dataList.get(i).getCoin().length; i++) {
                    loadDataCoin(dataList.get(j).getBv(),dataList.get(i).getCoin()[i]);
                }
                for (int i = 0; i < dataList.get(i).getFavorite().length; i++) {
                    loadDataFavorite(dataList.get(j).getBv(),dataList.get(i).getFavorite()[i]);
                }
                for (int i = 0; i < dataList.get(i).getViewerMids().length; i++) {
                    loadDataView(dataList.get(j).getBv(),dataList.get(j).getViewerMids()[i],dataList.get(i).getViewTime()[j]);
                }
                num++;
                if (num %30 == 0) {
                    try {
                        stmt.executeBatch();
                        stmt.clearBatch();
                        stmtLike.executeBatch();
                        stmtLike.clearBatch();
                        stmtCoin.executeBatch();
                        stmtCoin.clearBatch();
                        stmtFar.executeBatch();
                        stmtFar.clearBatch();
                        stmtView.executeBatch();
                        stmtView.clearBatch();
                        mycommit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
//                System.out.println(num+"视频");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
        try {
            stmt.executeBatch();
            stmt.clearBatch();
            stmtCoin.executeBatch();
            stmtCoin.clearBatch();
            stmtLike.executeBatch();
            stmtLike.clearBatch();
            stmtFar.executeBatch();
            stmtFar.clearBatch();
            stmtView.executeBatch();
            stmtView.clearBatch();
            mycommit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("finish");
        long cur=System.currentTimeMillis()-start;
        System.out.println("时间"+cur);
    }

    private void loadDataCoin(String BV, long user_mid) throws SQLException {
        if (connection != null) {
            stmtCoin.setString(1, BV);
            stmtCoin.setLong(2, user_mid);
            stmtCoin.addBatch();
        }
    }

    private void loadDataLike(String BV, long user_mid) throws SQLException {
        if (connection != null) {
            stmtLike.setString(1, BV);
            stmtLike.setLong(2, user_mid);
            stmtLike.addBatch();
        }
    }

    private void loadDataFavorite(String BV, long user_mid) throws SQLException {
        if (connection != null) {
            stmtFar.setString(1, BV);
            stmtFar.setLong(2, user_mid);
            stmtFar.addBatch();
        }
    }

    private void loadDataView(String BV, long user_mid, float watch_time) throws SQLException {
        if (connection != null) {
            stmtView.setString(1, BV);
            stmtView.setLong(2, user_mid);
            stmtView.setFloat(3, watch_time);
            stmtView.addBatch();
        }
    }

    private void loadDataVideo(String BV, String title, long owner_mid, String owner_name,
                               Timestamp commit_time, Timestamp review_time, Timestamp public_time,
                               double duration, String description, long reviewer_mid) throws SQLException {
        if (connection != null) {
            stmt.setString(1, BV);
            stmt.setString(2, title);
            stmt.setLong(3, owner_mid);
            stmt.setString(4, owner_name);
            stmt.setTimestamp(5, commit_time);
            stmt.setTimestamp(6, review_time);
            stmt.setTimestamp(7, public_time);
            stmt.setDouble(8, duration);
            stmt.setString(9, description);
            stmt.setLong(10, reviewer_mid);
            stmt.addBatch();
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

