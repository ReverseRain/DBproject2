package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.tread.InsertThreadDanmu;
import io.sustc.service.tread.InsertThreadUser;
import io.sustc.service.tread.InsertThreadVideo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class databaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
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
    public List<Integer> getGroupMembers() {

        return Arrays.asList(12210624,12210626);

    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        //partI to insert user and follow tables
        ExecutorService executorServiceUser= Executors.newFixedThreadPool(10000);
        for (int i = 0; i < userRecords.toArray().length; i+=1300) {
            InsertThreadUser insertThreadUser=new InsertThreadUser(con,
                    userRecords.subList(i,Math.min(i + 1300, userRecords.size())));
            executorServiceUser.execute(insertThreadUser);
        }
        executorServiceUser.shutdown();
        //partII to insert videos and so on
        ExecutorService executorServiceVideo= Executors.newFixedThreadPool(100);
        for (int i = 0; i < videoRecords.toArray().length; i+=13000) {
            InsertThreadVideo insertThreadVideo=new InsertThreadVideo(con,
                    videoRecords.subList(i,Math.min(i + 13000, videoRecords.size())),System.currentTimeMillis());
            executorServiceVideo.execute(insertThreadVideo);
        }
        executorServiceVideo.shutdown();
        //partIII to insert danmu and so on
        ExecutorService executorServiceDanmu= Executors.newFixedThreadPool(10000);
        for (int i = 0; i < danmuRecords.toArray().length; i+=1300) {
            InsertThreadDanmu insertThreadDanmu=new InsertThreadDanmu(con,
                    danmuRecords.subList(i,Math.min(i + 1300, danmuRecords.size())),System.currentTimeMillis());
            executorServiceDanmu.execute(insertThreadDanmu);
        }
        executorServiceDanmu.shutdown();

        // implement your import logic
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());

    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
