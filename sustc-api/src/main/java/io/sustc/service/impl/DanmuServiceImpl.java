package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
@Service
public class DanmuServiceImpl implements DanmuService {
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

    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (!auth.isValid(con)){
            return -1;
        }
        if (bv==null||bv.equals("")){
            return -1;
        }
        if (content==null||content.equals("")){
            return -1;
        }

        try {
            String sql="select * from view where bv="+bv+"and mid="+auth.getMid();
            String sql2="select * from videos where bv="+bv;
            PreparedStatement stmt= con.prepareStatement(sql);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                return -1;
            }
            stmt=con.prepareStatement(sql2);
            rs=stmt.executeQuery();
            Date data = new Date();
            Timestamp current = new Timestamp(data.getTime());
            if (!rs.next()){
                return -1;
            }else if (!rs.getTimestamp("publicTime").before(current)){
                return -1;
            }else {
                String sqlFinal="insert into danmu(bv,mid,time,content,postTime) " +
                        "values("+bv+","+auth.getMid()+","+time+","+content+","+current+")";
                stmt= con.prepareStatement(sqlFinal);
                stmt.execute();
                sql2="select count(*) from danmu";
                stmt= con.prepareStatement(sql2);
                rs=stmt.executeQuery();
                rs.next();
                return rs.getInt("id");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        List<Long> ans = new ArrayList<>();
        String sql = null;

//       if (timeStart>=timeEnd||timeEnd<0||timeStart<0||timeStart>)
        //corner case
        if (filter) {
            sql = "select * from danmu a where " +
                    "timeStart==(select min(timeStart) from danmu  where a.content==b.content) and " +
                    "bv==" + bv + " and (time between " + timeStart + " and " + timeEnd + ") order by time";
        } else {
            sql = "select * from danmu where bv==" + bv +
                    " and (time between " + timeStart + " and " + timeEnd + ") order by time";
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                ans.add(rs.getLong("mid"));
            }
            return ans;//sorting by time?
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if(!auth.isValid(con)){
            return false;
        }
        if(id<=0){
            return false;
        }
        String sql="select * from danmu where id="+id;
        try {
            PreparedStatement stmt=con.prepareStatement(sql);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                return false;
            }else {
                sql="select * from danmuLikeBy where danmuID="+id+"and likeMid="+auth.getMid();
                stmt=con.prepareStatement(sql);
                rs=stmt.executeQuery();
                if (rs.next()){
                    sql="delete * from danmuLikeBy where danmuID="+id+" and likeMid="+auth.getMid();
                }else {
                    sql="insert into danmuLikeBy" +
                            "(danmuID,likeMid) values("+id+","+auth.getMid()+")";
                }
                stmt=con.prepareStatement(sql);
                return stmt.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

