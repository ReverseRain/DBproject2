package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;
    private Connection con;

    {
        try {
            con = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {

        if (!auth.isValid()){
            return null;
        }
        Date currentDate = new Date();

        Timestamp currentTimestamp = new Timestamp(currentDate.getTime());

        if (!req.isValid(currentTimestamp,dataSource,auth)){
            return null;
        }
        //此处应该返回一个BV？
        return req.getTitle();
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (!auth.isValid()){
            return false;
        }
        if (bv==null){
            return false;
        }
        try {
            String sql1="select * from video where bv=="+bv;
            String sql2="select * from video where ownerMid=="+auth.getMid()+" and bv =="+bv;
            PreparedStatement stmt= con.prepareStatement(sql1);
            ResultSet rs= stmt.executeQuery();
            if (!rs.next()){
                return false;
            }
            stmt=con.prepareStatement(sql2);
            rs=stmt.executeQuery();
            //to judge whether auth is a superuser
//            if (!rs.next()&&auth.){
//                return false;
//            }

            String delete="delete * from video where bv=="+bv;
            stmt=con.prepareStatement(delete);
            return stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        Date data=new Date();
        Timestamp time=new Timestamp(data.getTime());
        if (!auth.isValid()){
            return false;
        }
        if (bv==null){
            return false;
        }
        try {
            String sql1="select * from video where bv=="+bv;
            String sql2="select * from video where ownerMid=="+auth.getMid()+" and bv =="+bv;
            PreparedStatement stmt=con.prepareStatement(sql1);
            ResultSet rs= stmt.executeQuery();
            if (!rs.next()){
                return false;
            }else if (req.getDuration()!=rs.getLong("duration")){
                return false;
            } else if (!req.getTitle().equals(rs.getString("title"))
                    || req.getDescription().equals(rs.getString("description"))
                    ) {
                return false;
            }
            stmt=con.prepareStatement(sql2);
            rs=stmt.executeQuery();
            if (!rs.next()){
                return false;
            }
            if (!req.isValid(time,dataSource,auth)){
                return false;
            }
            //update bv what?
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {

        return null;
    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
