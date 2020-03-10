import Crypto.Aes256Class;
import org.apache.log4j.Logger;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class Insert_GP {

    final static Logger logger = Logger.getLogger(Insert_GP.class.getName());
    private static String url;
    public static String password;
    private static boolean isConnected = false;

    public String dt_load;
    public Date Check_Oracle_date;
    public Date Check_PG_date;

    public void connect(String GP_server,String GP_port,String GP_sid,String GP_username,String  GP_driverName,
                        String ORA_server,String ORA_port,String ORA_service_name,String ORA_username,String ORA_driverName,
                        String HIVE_server,String HIVE_port,String HIVE_sid,String HIVE_username,String HIVE_password,String HIVE_driverName) {

        try {
            byte[] in_pass = Files.readAllBytes(Paths.get("D:\\IPTV\\GP.txt"));
            password = Aes256Class.getInstance().decrypt(new String(in_pass));
        } catch (IOException e) {
            logger.error("IOException \n" + e.getMessage() + e.getStackTrace());
            //System.out.println("IOException\n" + e.getMessage());
        }

        try {

            url = "jdbc:postgresql://" + GP_server + ":" + GP_port + "/" + GP_sid;
            logger.info(url);
            //System.out.println(url);
            Class.forName(GP_driverName);
            Connection con = DriverManager.getConnection(url, GP_username, password);
            logger.info("connected: " + url);
            //System.out.println("connected: " + url);
            con.setAutoCommit(false);

            String sql_check_pg_date = "select max(to_char(dt,'yyyy-mm-dd')) from iptv_zabbix.iptv_zabbix_chk_traffic";

            PreparedStatement ps_pg = con.prepareStatement(sql_check_pg_date);
            ResultSet rs_pg = ps_pg.executeQuery();

            while (rs_pg.next()) {
                Check_PG_date= rs_pg.getDate(1);
            }

            Check_data Check_data = new Check_data();
            Check_data.connect(ORA_server, ORA_port, ORA_service_name, ORA_username, ORA_driverName);
            Check_Oracle_date = Check_data.dt_dbview_check;

            try {
                while(Check_PG_date.getTime()<Check_Oracle_date.getTime()) {
                    //Thread.sleep(40);
                    Check_data.connect(ORA_server, ORA_port, ORA_service_name, ORA_username, ORA_driverName);
                    logger.info("Current date in Oracle is :" + Check_Oracle_date + " and in PG is :" + Check_PG_date);
                    //System.out.println("Current date in Oracle is :" + Check_Oracle_date + " and in PG is :" + Check_PG_date);

                    Calendar cal = Calendar.getInstance();
                    cal.setTime(Check_PG_date);
                    cal.add(Calendar.DATE,1);

                    dt_load = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                    java.util.Date Check_PG_date_util = format.parse(dt_load);
                    Check_PG_date= new java.sql.Date(Check_PG_date_util.getTime());
                    logger.info("Check_PG_date is : " + Check_PG_date);
                    //System.out.println("Check_PG_date is : " + Check_PG_date);


                    Load_Hive Load_Hive = new Load_Hive();
                    Load_Hive.connect(dt_load,HIVE_server,HIVE_port,HIVE_sid,HIVE_username,HIVE_password,HIVE_driverName);
                    int Load_Hive_length = Load_Hive.Cur_hive_traffic.size();

                    Statement stmt = con.createStatement();
                    int result = stmt.executeUpdate("TRUNCATE TABLE iptv_zabbix.iptv_zabbix_chk_traffic_stg");
                    con.commit();
                    logger.info("Status for TRUNCATE TABLE is: " + result);
                    //System.out.println("Status for TRUNCATE TABLE is: " + result);

                    try {
                        String sql = "Insert into iptv_zabbix.iptv_zabbix_chk_traffic_stg( local_date,\n" +
                                "  local_date_h,\n" +
                                "  region_id,\n" +
                                "  subregion_id,\n" +
                                "  c_address,\n" +
                                "  traffic_volume) values(?,?,?,?,?,?)";

                        PreparedStatement ps = con.prepareStatement(sql);

                        for (int i = 1; i < Load_Hive_length; i++) {

                            String[] a1 = Load_Hive.Cur_hive_traffic.get(i).split(",");
                            String[] a1_1 = a1[0].split(" ");

                            ps.setString(1, a1_1[0]);
                            ps.setString(2, a1_1[1]);
                            ps.setString(3, a1[1]);
                            ps.setString(4, a1[2]);
                            ps.setString(5, a1[3]);
                            ps.setString(6, a1[4]);

                            result = ps.executeUpdate();
                            /*if (i%2000==0) {
                                System.out.println("Count str is :" + i);
                                con.commit();
                            }*/

                        }

                        con.commit();
                        logger.info("Insert complite");
                        //System.out.println("Insert complite");

                        CallableStatement cstmt = con.prepareCall("select iptv_zabbix.iptv_zabbix_calc_chk_traffic(date '" + dt_load + "');");
                        cstmt.execute();
                        con.commit();
                        logger.info("Function iptv_zabbix.iptv_zabbix_calc_chk_traffic complite");
                        //System.out.println("Function iptv_zabbix.iptv_zabbix_calc_chk_traffic complite");

                    } catch (SQLException e) {
                        logger.error("SQLException \n" + e.getMessage() + e.getStackTrace());
                        //System.out.println("SQLException\n" + e.getMessage());
                        isConnected = false;
                    }
                }
                if (Check_PG_date.getTime()>=Check_Oracle_date.getTime()) {
                    logger.warn("Nothing to do - PG_date: " + Check_PG_date + " >= " + "Oracle_date: " + Check_Oracle_date);
                    //System.out.println("PG_date: " + Check_PG_date + " >= " + "Oracle_date: " + Check_Oracle_date);
                }
                //}

            }catch (ParseException e) {
                logger.error("ParseException \n" + e.getMessage() + e.getStackTrace());
                //System.out.println("ParseException\n" + e.getMessage());
                isConnected = false;
            } finally {
                con.close();
            }

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
