import java.sql.*;
import java.util.Date;

import com.facebook.presto.jdbc.PrestoDriver;
import com.facebook.presto.jdbc.internal.airlift.slice.DynamicSliceOutput;

//import presto jdbc driver packages here.  
public class PrestoJdbcSample {  
   public static void main(String[] args) {  
      Connection connection = null; 
      Statement statement = null;  
      try {
         Class.forName("com.facebook.presto.jdbc.PrestoDriver");

         // Aqui tem que colocar o IP do POD (presto-coordinator) que fica no namespace "presto"
         // O IP tem que ser o do endpoint do presto-coordinator
         connection = DriverManager.getConnection("jdbc:presto://presto@XXX.XX.XXX.XX:8080/iceberg/bronze", "root", "");

         //connect mysql server tutorials database here 
         statement = connection.createStatement(); 
         String sql;
         sql = "select year, month, count(1) as total from person group by year, month";

         System.out.println("Inicio: "  + new Date().toString());
         ResultSet resultSet = statement.executeQuery(sql);

         int i = 0;
         while( resultSet.next() ){
            String year    = resultSet.getString(1);
            String month   = resultSet.getString(2);
            String tot     = resultSet.getString(3);
            System.out.println(String.format("Year: %s | Month: %s | Total: %s", year, month, tot));
         }
         System.out.println("Fim: " + new Date().toString());

         resultSet.close(); 
         statement.close(); 
         connection.close(); 
         
      }catch(SQLException sqlException){ 
         sqlException.printStackTrace(); 
      }catch(Exception exception){ 
         exception.printStackTrace(); 
      }
   } 
}