import Crypto.Aes256Class;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class Check_data {
    final static org.apache.log4j.Logger logger = Logger.getLogger(Check_data.class.getName());
    private static String url;
    public static String password;
    private static boolean isConnected = false;

    public Date dt_dbview_check;

    public void connect(String ORA_server,String ORA_port,String ORA_service_name,String ORA_username,String  ORA_driverName) {

        try {
            byte[] in_pass = Files.readAllBytes(Paths.get("D:\\IPTV\\Oracle.txt"));
            password = Aes256Class.getInstance().decrypt(new String(in_pass));
        } catch (IOException e) {
            logger.error("IOException \n" + e.getMessage() + e.getStackTrace());
            //System.out.println("IOException\n" + e.getMessage());
        }

        try {
            url = "jdbc:oracle:thin:@" + ORA_server + ":" + ORA_port + "/" + ORA_service_name;
            logger.info(url);
            //System.out.println(url);
            Class.forName(ORA_driverName);
            Connection con = DriverManager.getConnection(url, ORA_username, password);
            logger.info("connected: " + url);
            //System.out.println("connected: " + url);

            String sql = "select dt from (\n" +
                    "     select to_char(dt,'yyyy-mm-dd') dt,row_number() over(order by dt desc) rn from IPTV_MD.etl_smartspy_load_control t where fact_name='sdp_log' \n" +
                    "     and to_char(dt,'hh24:mi:ss')='23:00:00' and status='SUCCESS' )\n" +
                    "     where rn=1";

            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                dt_dbview_check= rs.getDate(1);
            }
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