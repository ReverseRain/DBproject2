package io.sustc.dto;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The authorization information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthInfo implements Serializable {

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;
    public boolean isValid(Connection con){
        if (mid<=0||(qq==null&&wechat==null)){
            return false;
        }
        String sql=null;
        if (qq!=null&&wechat!=null){
             sql="select * from user where qq="+qq+"and wechat="+wechat;
        }else if (qq!=null){
             sql="select * from user where qq="+qq;
        } else if (wechat!=null) {
             sql="select * from user where wechat="+wechat;
        }
        try {
            PreparedStatement stmt= con.prepareStatement(sql);
            ResultSet rs= stmt.executeQuery();
            if (!rs.next()){
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
    public boolean isSuperUser(Connection con){
        String sql="select * from user where mid="+mid;
        try {
            PreparedStatement stmt= con.prepareStatement(sql);
            ResultSet rs= stmt.executeQuery();
            if (rs.next()&&
                    !rs.getString("identity").equals("superuser")){
                return false;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
