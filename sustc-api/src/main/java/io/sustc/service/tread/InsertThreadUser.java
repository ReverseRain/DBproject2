package io.sustc.service.tread;

import io.sustc.dto.UserRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class InsertThreadUser implements Runnable {
    private Connection connection;
    private PreparedStatement stmt;
    private PreparedStatement stmtFollow;
    private List<UserRecord> dataList;

    public InsertThreadUser(Connection connection, List<UserRecord> dataList) {
        this.connection = connection;
        try {
            stmt = connection.prepareStatement("insert into UserRecord(Mid,Name,Sex,Birthday,Level,Sign,identity,coin,password,qq,wechat)" + "values(?,?,?,?,?,?,?,?,?,?,?)");
            stmtFollow=connection.prepareStatement("insert into Followings(user_mid,following_mid)" + "values(?,?)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.dataList = dataList;
    }

    @Override
    public void run() {
        long num = 0;
        for (int i = 0; i < dataList.size(); i++) {
            try {
                loadDataUser(dataList.get(i).getMid(), dataList.get(i).getName(), dataList.get(i).getSex(),
                        dataList.get(i).getBirthday(), dataList.get(i).getLevel(), dataList.get(i).getSign(), dataList.get(i).getIdentity(),
                        dataList.get(i).getCoin(),dataList.get(i).getPassword(),dataList.get(i).getQq(),dataList.get(i).getWechat());
                for (int j = 0; j < dataList.get(i).getFollowing().length; j++) {
                    loadDataFollowing(dataList.get(i).getMid(),dataList.get(i).getFollowing()[j]);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            num++;
            if (num % 30 == 0) {
                try {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    stmtFollow.executeBatch();
                    stmtFollow.clearBatch();
                    mycommit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
//            System.out.println(num+"用户");
        }
        try {
            stmt.executeBatch();
            stmt.clearBatch();
            stmtFollow.executeBatch();
            stmtFollow.clearBatch();
            mycommit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadDataUser(long mid, String name, String sex, String birthday, short level,
                              String sign,Enum identity,int coin,String password,String qq,String wechat) throws SQLException {
        if (connection != null) {
            stmt.setLong(1, mid);
            stmt.setString(2, name);
            stmt.setString(3, sex);
            stmt.setString(4, birthday);
            stmt.setShort(5, level);
            stmt.setString(6, sign);
            //set enum?
            stmt.setString(7, String.valueOf(identity));
            stmt.setInt(8,coin);
            stmt.setString(9, password);
            stmt.setString(10,qq);
            stmt.setString(11,wechat);
            stmt.addBatch();
        }
    }
    private void loadDataFollowing(long mid, long following_mid) throws SQLException {
        if (connection != null) {
            stmtFollow.setLong(1, mid);
            stmtFollow.setLong(2, following_mid);
            stmtFollow.addBatch();
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

