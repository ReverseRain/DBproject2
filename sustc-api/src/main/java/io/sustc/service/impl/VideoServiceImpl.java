package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
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

        if (!auth.isValid(con)) {
            return null;
        }
        Date currentDate = new Date();

        Timestamp currentTimestamp = new Timestamp(currentDate.getTime());

        if (!req.isValid(currentTimestamp, dataSource, auth)) {
            return null;
        }
        //此处应该返回一个BV？
        return req.getTitle();
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (!auth.isValid(con)) {
            return false;
        }
        if (bv == null) {
            return false;
        }
        try {
            String sql1 = "select * from video where bv==" + bv;
            String sql2 = "select * from video where ownerMid==" + auth.getMid() + " and bv ==" + bv;
            PreparedStatement stmt = con.prepareStatement(sql1);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return false;
            }
            stmt = con.prepareStatement(sql2);
            rs = stmt.executeQuery();
            //to judge whether auth is a superuser
            if (!rs.next() && auth.isSuperUser(con)) {
                return false;
            }
            String delete = "delete * from video where bv==" + bv;
            stmt = con.prepareStatement(delete);
            return stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        Date data = new Date();
        Timestamp time = new Timestamp(data.getTime());
        if (!auth.isValid(con)) {
            return false;
        }
        if (bv == null) {
            return false;
        }
        try {
            String sql1 = "select * from video where bv =" + bv;
            String sql2 = "select * from video where ownerMid =" + auth.getMid() + " and bv ==" + bv;
            PreparedStatement stmt = con.prepareStatement(sql1);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return false;
            } else if (req.getDuration() != rs.getLong("duration")) {
                return false;
            } else if (req.getTitle().equals(rs.getString("title"))
                    && req.getDescription().equals(rs.getString("description"))
            ) {
                return false;
            } else if (rs.getString("reviewer") == null) {
                return false;
            }
            stmt = con.prepareStatement(sql2);
            rs = stmt.executeQuery();
            if (!rs.next()) {
                return false;
            }
            if (!req.isValid(time, dataSource, auth)) {
                return false;
            }
            String sqlFinal = "update video set title =" + req.getTitle()
                    + ",description =" + req.getDescription() + ",reviewer =" + auth.getMid()
                    + "where bv=" + bv;
            stmt = con.prepareStatement(sqlFinal);
            return stmt.execute();
            //update bv what?
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (!auth.isValid(con)) {
            return null;
        }
        if (keywords == null || keywords.equals(" ")) {
            return null;
        }
        if (pageSize <= 0 || pageNum <= 0) {
            return null;
        }
        //unreviewed?
        //page size?
        String[] keyword = keywords.split(" ");
        StringBuilder sb = new StringBuilder();
        sb.append("select bv,reviewer,ownerMid,(select count(*) from view where bv=bv)as cnt from (select (");
        for (int i = 0; i < keyword.length; i++) {
            sb.append("+(char_length(title)" +
                    "-char_length(replace(title,'" + keyword[i] + "',''))+" +
                    "char_length(description)" + "-char_length(replace(description,'" + keyword[i] + "',''))" +
                    "char_length(ownerName)" + "-char_length(replace(ownerName,'" + keyword[i] + "','')))/"+keyword[i].length());
        }
        sb.append(") as num from" +
                "*,video)a where num>0 order by num desc,cnt desc");
        //if num is same,order by view
        ArrayList<String> ans = new ArrayList<>();
        try {
            PreparedStatement stmt = con.prepareStatement(sb.toString());
            ResultSet rs = stmt.executeQuery();
            Timestamp publicTime;
            Date currentDate = new Date();
            Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
            while (rs.next()) {
                publicTime=rs.getTimestamp("publicTime");
                if (rs.getString("reviewer") != null&&publicTime.before(currentTimestamp)) {
                    ans.add(rs.getString("bv"));
                } else if (auth.isSuperUser(con)) {
                    ans.add(rs.getString("bv"));
                } else if (auth.getMid()==rs.getLong("ownerMid")) {
                    ans.add(rs.getString("bv"));
                }
            }
            return ans;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public double getAverageViewRate(String bv) {
        if (bv == null) {
            return -1;
        }
        String sql1 = "select * from view where bv =" + bv;
        try {
            PreparedStatement stmt = con.prepareStatement(sql1);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return -1;
            } else {
                int count = 0;
                double ans = 0;
                do {
                    ans += rs.getDouble("watchTime");
                    count++;
                } while (rs.next());
                ans = ans / count;
                String sqlGetDuration = "select * from video where bv=" + bv;
                stmt = con.prepareStatement(sqlGetDuration);
                rs = stmt.executeQuery();
                rs.next();
                ans = ans / (rs.getDouble("duration"));
                return ans;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Integer> getHotspot(String bv) {

        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (!auth.isValid(con)) {
            return false;
        }
        if (bv == null) {
            return false;
        }
        String sql1 = "select * from video where bv =" + bv;
        try {
            PreparedStatement stmt = con.prepareStatement(sql1);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return false;
            } else if (rs.getString("reviewer") != null) {
                return false;
            } else if (auth.getMid() == rs.getLong("ownerMid")) {
                return false;
            } else if (!auth.isSuperUser(con)) {
                return false;
            }
            String sqlFinal = "update video set isReview= true";
            stmt = con.prepareStatement(sqlFinal);
            return stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        if (bv==null||bv.equals("")){
            return false;
        }
        String bvValid="select * from video where bv="+bv;
        try {
            PreparedStatement stmt=con.prepareStatement(bvValid);
            ResultSet rs=stmt.executeQuery();
            Date currentDate = new Date();
            Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
            if (!rs.next()){
                return false;
            } else if (rs.getString("reviewer")==null||!rs.getTimestamp("pubicTime").before(currentTimestamp)) {
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        //search video?
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
