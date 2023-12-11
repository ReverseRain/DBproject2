package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;

    public long sendDanmu(AuthInfo auth, String bv, String content, float time){
return 1;
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
       List<Long>ans=new ArrayList<>();
       String sql=null;

//       if (timeStart>=timeEnd||timeEnd<0||timeStart<0||timeStart>)
        //corner case
        if (filter){
             sql="select * from danmu a where " +
                    "timeStart==(select min(timeStart) from danmu  where a.content==b.content) and " +
                    "bv=="+bv+" and (time between " + timeStart + " and " + timeEnd + ") order by time";
        }else {
             sql = "select * from danmu where bv==" + bv +
                    " and (time between " + timeStart + " and " + timeEnd + ") order by time";
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                ans.add(rs.getLong("mid"));
            }
            return ans;//sorting by time?
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {

        return false;
    }
}

