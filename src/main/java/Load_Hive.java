import Crypto.Aes256Class;
import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Load_Hive {
    final static org.apache.log4j.Logger logger = Logger.getLogger(Load_Hive.class.getName());
    private static String url;
    private static String password;
    private static boolean isConnected = false;

    public List<String> Cur_hive_traffic = new ArrayList<String>(); //  массив для заполнения выборкой из Hive

    public void connect(String dt_load,String HIVE_server,String HIVE_port,String HIVE_sid,String HIVE_username,String HIVE_password,String HIVE_driverName) throws SQLException {

        try {
            byte[] in_pass = Files.readAllBytes(Paths.get("D:\\IPTV\\Hive.txt"));
            password = Aes256Class.getInstance().decrypt(new String(in_pass));
        } catch (IOException e) {
            logger.error("IOException \n" + e.getMessage() + e.getStackTrace());
            //System.out.println("IOException\n" + e.getMessage());
        }

        try {
            url = "jdbc:hive2://" + HIVE_server + ":" + HIVE_port + "/;ssl=0";
            logger.info(url);
            //System.out.println(url);
            Class.forName(HIVE_driverName);
            Connection con = DriverManager.getConnection(url, HIVE_username, HIVE_password);
            logger.info("connected: " + url);
            //System.out.println("connected: " + url);

            String sql = "select from_unixtime(unix_timestamp(concat(ymd, ' ', hour, ':00:00')) + case\n" +
                    "                               when c_address = '10.160.160.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.204.0.49' then\n" +
                    "                                7 * 3600\n" +
                    "                               when c_address = '10.184.124.19' then\n" +
                    "                                2 * 3600\n" +
                    "                               when c_address = '10.143.60.11' then\n" +
                    "                                4 * 3600\n" +
                    "                               when c_address = '10.36.198.139' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.68.15.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.144.35.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.32.144.187' then\n" +
                    "                                0 * 3600\n" +
                    "                             end,\n" +
                    "                             'yyyy-MM-dd HH') as local_date,\n" +
                    "                             region_id,\n" +
                    "       subregion_id,                             \n" +
                    "       c_address,\n" +
                    "       sum(floor(((substr(`interval`, 0, 2) * 60 +\n" +
                    "                 translate(substr(`interval`,4), ',', '.')) * iface_bitrate) / 8)) as traffic_volume\n" +
                    "from iptv_ddl.iptv_smartspy_log \n" +
                    " where c_address in ('10.160.160.11',\n" +
                    "                     '10.204.0.49',\n" +
                    "                     '10.184.124.19',\n" +
                    "                     '10.143.60.11',\n" +
                    "                     '10.36.198.139',\n" +
                    "                     '10.68.15.11',\n" +
                    "                     '10.144.35.11',\n" +
                    "                     '10.32.144.187')\n" +
                    "   and ((ymd = date_add('" + dt_load + "', -1) and hour >= '17') or\n" +
                    "       ymd = '" + dt_load + "')\n" +
                    "   and TO_DATE(from_unixtime(unix_timestamp(concat(ymd, ' ', hour, ':00:00')) + case\n" +
                    "                               when c_address = '10.160.160.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.204.0.49' then\n" +
                    "                                7 * 3600\n" +
                    "                               when c_address = '10.184.124.19' then\n" +
                    "                                2 * 3600\n" +
                    "                               when c_address = '10.143.60.11' then\n" +
                    "                                4 * 3600\n" +
                    "                               when c_address = '10.36.198.139' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.68.15.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.144.35.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.32.144.187' then\n" +
                    "                                0 * 3600\n" +
                    "                             end,\n" +
                    "                             'yyyy-MM-dd')) = '" + dt_load + "'\n" +
                    "   and length(mac) = 17\n" +
                    " group by from_unixtime(unix_timestamp(concat(ymd, ' ', hour, ':00:00')) + case\n" +
                    "                               when c_address = '10.160.160.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.204.0.49' then\n" +
                    "                                7 * 3600\n" +
                    "                               when c_address = '10.184.124.19' then\n" +
                    "                                2 * 3600\n" +
                    "                               when c_address = '10.143.60.11' then\n" +
                    "                                4 * 3600\n" +
                    "                               when c_address = '10.36.198.139' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.68.15.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.144.35.11' then\n" +
                    "                                0 * 3600\n" +
                    "                               when c_address = '10.32.144.187' then\n" +
                    "                                0 * 3600\n" +
                    "                             end,\n" +
                    "                             'yyyy-MM-dd HH'),\n" +
                    "                             region_id,\n" +
                    "       subregion_id,\n" +
                    "          c_address\n";

            Statement stmt_hive_mr = con.createStatement();
            // mapred-site.xml.
            String mr_set = "set hive.mapred.mode=nonstrict;\n " +                                           // query all data without predicate for partitions
                    "       set hive.exec.dynamic.partition.mode=nonstrict;\n " +                            // insert data dynamically into your partitioned table if true
                    "       set mapreduce.map.speculative=true;\n " +                                        //  If true, then multiple instances of some map tasks may be executed in parallel. Default is true.
                    "       set mapreduce.reduce.speculative=true;\n" +                                      //  If true, then multiple instances of some reduce tasks may be executed in parallel. Default is true.
                    "       set mapreduce.input.fileinputformat.split.minsize=500000000;\n " +
                    "       set mapreduce.input.fileinputformat.split.maxsize=1000000000;\n " +
                    "       set mapreduce.input.fileinputformat.split.minsize.per.node=500000000;\n " +
                    "       set mapreduce.input.fileinputformat.split.minsize.per.rack=500000000;";

            stmt_hive_mr.execute(mr_set);
            logger.info("Mapred set succesfully");
            //System.out.println("Mapred set succesfully");

            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String a1 = rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5);
                Cur_hive_traffic.add(a1);
            }
            logger.info("Data take succesfully");
            //System.out.println("Data take succesfully");

            con.close();
        }   catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException \n" + e.getMessage() + e.getStackTrace());
            //System.out.println("ClassNotFoundException\n" + e.getMessage());
            isConnected = false;
        }
        catch (SQLException e) {
            logger.error("SQLException \n" + e.getMessage() + e.getStackTrace());
            //System.out.println("SQLException\n" + e.getMessage());
            isConnected = false;
        }
    }
}