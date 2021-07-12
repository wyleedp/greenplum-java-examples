package wyleedp.greenplum.java.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;

/**
 * Greenplum 레프커서 실행예제
 *   - 준비
 *     1. /src/main/resources/wyleedp/greenplum/java/examples/GreenplumRefcursorExample.sql 를 실행하여 public.ref_fun_test function 이 생성되어 있어야 한다.
 *     2. 실행할 Greenplum 접속정보를 수정한다. (GP_URL, GP_USERNAME, GP_PASSWORD)
 */
public class GreenplumRefcursorExample {

	private static final String GP_DRIVER_CLASSNAME = "org.postgresql.Driver";
	private static final String GP_URL = "jdbc:postgresql://192.168.2.141/postgres";
	private static final String GP_USERNAME = "gpadmin";
	private static final String GP_PASSWORD = "changeme";

	public void execution() {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			Class.forName(GP_DRIVER_CLASSNAME);
			con = DriverManager.getConnection(GP_URL, GP_USERNAME, GP_PASSWORD);
			st = con.createStatement();
			rs = st.executeQuery("select version()");
			
			// Greenplum Version 정보출력
			while(rs.next()) {
				System.out.println("Greenplum Version : " + rs.getString(1));
			}
			
			// 레프커서 실행
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT * FROM public.ref_fun_test();");
			sqlBuilder.append("FETCH ALL FROM refcursor_1;");
			sqlBuilder.append("FETCH ALL FROM refcursor_2;");
			sqlBuilder.append("FETCH ALL FROM refcursor_3;");

			boolean isExecution = st.execute(sqlBuilder.toString());
			
			int sqlIndex = 0;
			int columnWidth = 25;
			String line = StringUtils.repeat("-", 75);
			
			while(isExecution) {
				rs = st.getResultSet();
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();
				
				// 레프커서별 컬럼개수를 출력한다.
				System.out.println(sqlIndex + "] ColumnCount : " + columnCount);
				
				// 컬럼명을 출력한다.
				System.out.println(line);
				for(int columnIndex=1; columnIndex<=columnCount; columnIndex++) {
					String columnName = metaData.getColumnName(columnIndex);
					System.out.print(StringUtils.rightPad(columnName, columnWidth, " "));
				}
				System.out.println();
				System.out.println(line);
				
				// 데이터를 출력한다.
				while(rs.next()){
					for(int columnIndex=1; columnIndex<=columnCount; columnIndex++) {
						String columnTypeName = metaData.getColumnTypeName(columnIndex);
						
						if(StringUtils.equalsIgnoreCase(columnTypeName, "refcursor")) {
							// 컬럼유형이 레프커서이면 String 으로 출력한다.
							String columnValue = rs.getString(columnIndex);
							System.out.print(StringUtils.rightPad(columnValue, columnWidth, " "));
						}else {
							String columnValue = rs.getObject(columnIndex).toString();
							System.out.print(StringUtils.rightPad(columnValue, columnWidth, " "));
						}
					}
					System.out.println();
				}
				
				System.out.println();
				sqlIndex++;
				
				// 실행할 쿼리가 없으면 while 문을 빠져 나간다.
				if(!st.getMoreResults()) {
					break;
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) rs.close();
				if(st != null) st.close();
				if(con != null) con.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		GreenplumRefcursorExample example = new GreenplumRefcursorExample();
		example.execution();
	}

}
