import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class run {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        Properties props = new Properties();
        props.load(new FileInputStream("C:\\Users\\Aleksandr.A.Chernov\\IdeaProjects\\IPTV_zabbix_traffic\\out\\artifacts\\IPTV_zabbix_traffic_jar\\logs\\config.properties"));
        // GreenPlum
        String GP_server = props.getProperty("GP_server");
        String GP_port = props.getProperty("GP_port");;
        String GP_sid = props.getProperty("GP_sid");;
        String GP_username = props.getProperty("GP_username");;
        String GP_driverName = props.getProperty("GP_driverName");;
        //String GP_password;

        // Oracle
        String ORA_server = props.getProperty("ORA_server");
        String ORA_port = props.getProperty("ORA_port");;
        String ORA_service_name = props.getProperty("ORA_service_name");;
        String ORA_username = props.getProperty("ORA_username");;
        String ORA_driverName = props.getProperty("ORA_driverName");;

        // Hive
        String HIVE_server = props.getProperty("HIVE_server");
        String HIVE_port = props.getProperty("HIVE_port");;
        String HIVE_sid = props.getProperty("HIVE_sid");;
        String HIVE_username = props.getProperty("HIVE_username");;
        String HIVE_password = props.getProperty("HIVE_password");;
        String HIVE_driverName = props.getProperty("HIVE_driverName");;



        PropertyConfigurator.configure("C:\\Users\\Aleksandr.A.Chernov\\IdeaProjects\\IPTV_zabbix_traffic\\out\\artifacts\\IPTV_zabbix_traffic_jar\\logs\\log4j.properties");
        Insert_GP Insert_GP = new Insert_GP();
        Insert_GP.connect(GP_server,GP_port,GP_sid,GP_username,GP_driverName,ORA_server,ORA_port,ORA_service_name,ORA_username,ORA_driverName,
                HIVE_server,HIVE_port,HIVE_sid,HIVE_username,HIVE_password,HIVE_driverName);

    }
}