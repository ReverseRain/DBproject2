package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.Date;

@Service
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
        String alphabetsInUpperCase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String alphabetsInLowerCase = "abcdefghijklmnopqrstuvwxyz";
        String numbers = "0123456789";
        String allCharacters = alphabetsInLowerCase + alphabetsInUpperCase + numbers;
        StringBuffer randomString = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            int randomIndex = (int)(Math.random() * allCharacters.length());
            randomString.append(allCharacters.charAt(randomIndex));
        }
        String BV=randomString.toString();
        String sql="insert videos(bv,title,commitTime" +
                "publicTime,duration,description) values ("+BV+","+req.getTitle()+","+currentTimestamp+","
                +req.getPublicTime()+","+req.getDuration()+","+req.getDescription()+")";
        try {
            PreparedStatement stmt= con.prepareStatement(sql);
            stmt.execute();
            return BV;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //此处应该返回一个BV？
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
            String sql1 = "select * from videos where bv=" + bv;
            String sql2 = "select * from videos where ownerMid=" + auth.getMid() + " and bv =" + bv;
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
            String delete = "delete * from videos where bv=" + bv;
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
            String sql1 = "select * from videos where bv =" + bv;
            String sql2 = "select * from videos where ownerMid =" + auth.getMid() + " and bv =" + bv;
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
            String sqlFinal = "update videos set title =" + req.getTitle()
                    + ",description =" + req.getDescription() + ",reviewer =" + auth.getMid()
                    + "where bv =" + bv;
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
         Date currentDate = new Date();
         Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
        String[] keyword = keywords.split(" ");
        StringBuilder sb = new StringBuilder();
        sb.append("select bv,reviewer,ownerMid,(select count(*) from view where bv=bv)as cnt from (select (");
        for (int i = 0; i < keyword.length; i++) {
            sb.append("+(char_length(title)" +
                    "-char_length(replace(title,'" + keyword[i] + "',''))+" +
                    "char_length(description)" + "-char_length(replace(description,'" + keyword[i] + "',''))" +
                    "char_length(ownerName)" + "-char_length(replace(ownerName,'" + keyword[i] + "','')))/"+keyword[i].length());
        }
        sb.append(") as num , * from" +
                "videos)a where num>0 and ((reviewer is not null and publicTime before"
                +currentTimestamp+ ")or("+auth.isSuperUser(con)+") or" +
                " (ownerMid=" +auth.getMid()+")) order by num desc,cnt desc offset "+pageSize+" limit "+(pageNum-1)*pageSize);
        //compare the "offset limit" and rs?
        //if num is same,order by view
        ArrayList<String> ans = new ArrayList<>();
        try {
            PreparedStatement stmt = con.prepareStatement(sb.toString());
            ResultSet rs = stmt.executeQuery();
            Timestamp publicTime;
            ans= (ArrayList<String>) rs.getArray("bv");

            //compare this two methods.
//            while (rs.next()) {
//                publicTime=rs.getTimestamp("publicTime");
//                if (rs.getLong("reviewer") != null&&publicTime.before(currentTimestamp)) {
//                    ans.add(rs.getString("bv"));
//                } else if (auth.isSuperUser(con)) {
//                    ans.add(rs.getString("bv"));
//                } else if (auth.getMid()==rs.getLong("ownerMid")) {
//                    ans.add(rs.getString("bv"));
//                }
//            }
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
                //use function average
                sql1="select avg(watchTime) from view where bv="+bv;
                stmt=con.prepareStatement(sql1);
                rs=stmt.executeQuery();
                double ans=rs.getDouble("avg");
                //don't use avg function in sql and compare them
//                int count = 0;
//                double ans = 0;
//                do {
//                    ans += rs.getDouble("watchTime");
//                    count++;
//                }while (rs.next());
//                ans=ans/count;
                String sqlGetDuration = "select * from videos where bv=" + bv;
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
        //shi
        Set<Integer> ans=new LinkedHashSet<>();
        if(bv==null||bv.equals("")){
            return ans;
        }
        String sql="select * from danmu where BV="+bv;
        try {
            PreparedStatement stmt=con.prepareStatement(sql);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                return ans;
            }else {
                int start=0,end=(int)rs.getDouble("duration"),cnt=(int)(end/10),true_end=(int)rs.getDouble("duration");
                int max=0,ans1=0,ans2=0;
                String sql2="select count(*) from danmu where BV="+bv+" and time between "+start+" and "+end;
                for (int i = 0; i <cnt; i++) {
                    stmt= con.prepareStatement(sql2);
                    rs=stmt.executeQuery();
                    rs.next();
                    if (max<rs.getInt("count")){
                        ans1=start;
                        ans2=end;
                        max=rs.getInt("count");
                    }
                    start=end;end=Math.min(end+10,true_end);
                    sql2="select count(*) from danmu where BV="+bv+" and time between "+start+" and "+end;
                }
                ans.add(ans1);ans.add(ans2);
                return ans;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (!auth.isValid(con)) {
            return false;
        }
        if (bv == null) {
            return false;
        }
        String sql1 = "select * from videos where bv =" + bv;
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
            String sqlFinal = "update videos set review= "+auth.getMid();
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
        String bvValid="select * from videos where bv="+bv;
        try {
            PreparedStatement stmt=con.prepareStatement(bvValid);
            ResultSet rs=stmt.executeQuery();
            Date currentDate = new Date();
            Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
            if (!rs.next()){
                return false;
            } else if (rs.getString("reviewer")==null||!rs.getTimestamp("pubicTime").before(currentTimestamp)) {
                return false;
            } else if (auth.getMid()==rs.getLong("ownerMid")) {
                return false;
            }else {
                String sqlCoin="select * from UserRecord where mid="+auth.getMid();
                stmt=con.prepareStatement(sqlCoin);
                rs=stmt.executeQuery();
                int coin=0;
                if (rs.next()&&(coin=rs.getInt("coin"))>0){
                    String sql3="select * from coin(BV,mid) where BV="+bv+" and mid="+auth.getMid();
                    String sql1="insert into coin(BV,mid) values ("+bv+","+auth.getMid()+")";
                    String sql2="update UserRecord set coin="+(coin-1)+"where mid="+auth.getMid();
                    stmt=con.prepareStatement(sql3);
                    rs=stmt.executeQuery();
                    if (rs.next()){
                        return false;
                    }
                    stmt=con.prepareStatement(sql2);
                    stmt.execute();
                    stmt= con.prepareStatement(sql1);
                    return stmt.execute();
                }else {
                    return false;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //search video?

    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (bv==null||bv.equals("")){
            return false;
        }
        String bvValid="select * from videos where bv="+bv;
        try {
            PreparedStatement stmt=con.prepareStatement(bvValid);
            ResultSet rs=stmt.executeQuery();
            Date currentDate = new Date();
            Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
            if (!rs.next()){
                return false;
            } else if (rs.getString("reviewer")==null||!rs.getTimestamp("pubicTime").before(currentTimestamp)) {
                return false;
            } else if (auth.getMid()==rs.getLong("ownerMid")) {
                return false;
            }else {
                String sql2="select * from like where BV="+bv+" and mid="+auth.getMid();
                String sql1="insert into like(BV,mid) values ("+bv+","+auth.getMid()+")";
                String sql3="delete * from like where BV="+bv+" and mid="+auth.getMid();
                stmt=con.prepareStatement(sql2);
                rs=stmt.executeQuery();
                if (rs.next()){
                    stmt=con.prepareStatement(sql3);
                }else {
                stmt= con.prepareStatement(sql1);
                }
                return stmt.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (bv==null||bv.equals("")){
            return false;
        }
        String bvValid="select * from videos where bv="+bv;
        try {
            PreparedStatement stmt=con.prepareStatement(bvValid);
            ResultSet rs=stmt.executeQuery();
            Date currentDate = new Date();
            Timestamp currentTimestamp = new Timestamp(currentDate.getTime());
            if (!rs.next()){
                return false;
            } else if (rs.getString("reviewer")==null||!rs.getTimestamp("pubicTime").before(currentTimestamp)) {
                return false;
            } else if (auth.getMid()==rs.getLong("ownerMid")) {
                return false;
            }else {
                String sql2="select * from favorite where BV="+bv+" and mid="+auth.getMid();
                String sql1="insert into favorite(BV,mid) values ("+bv+","+auth.getMid()+")";
                String sql3="delete * from favorite where BV="+bv+" and mid="+auth.getMid();
                stmt=con.prepareStatement(sql2);
                rs=stmt.executeQuery();
                if (rs.next()){
                    stmt=con.prepareStatement(sql3);
                }else {
                    stmt= con.prepareStatement(sql1);
                }
                return stmt.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
