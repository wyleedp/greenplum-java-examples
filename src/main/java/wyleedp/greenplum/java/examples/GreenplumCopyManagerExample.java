package wyleedp.greenplum.java.examples;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * CopyManager를 이용한 CSV Import/Export 예제
 */
public class GreenplumCopyManagerExample {

	private static final String GP_DRIVER_CLASSNAME = "org.postgresql.Driver";
	private static final String GP_URL = "jdbc:postgresql://192.168.2.141/postgres?ApplicationName=GreenplumCopyManagerExample";
	private static final String GP_USERNAME = "gpadmin";
	private static final String GP_PASSWORD = "changeme";
	
	private Connection con = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	public GreenplumCopyManagerExample() throws Exception {
		Class.forName(GP_DRIVER_CLASSNAME);
		con = DriverManager.getConnection(GP_URL, GP_USERNAME, GP_PASSWORD);
	}
	
	public void close() throws Exception {
		if(rs != null) rs.close();
		if(stmt != null) stmt.close();
		if(con != null) con.close();
	}
	
	/**
	 * 사전준비
	 *   1. public.copy_manager_example 테이블이 존재하면 삭제
	 *   2. public.copy_manager_example 테이블 생성
	 * @throws Exception 
	 */
	public void prepare() throws Exception {
		String sql = "drop table if exists public.copy_manager_example;"
				+ "create table public.copy_manager_example("
				+ "    no          integer       not null"
				+ "  , title       varchar(300)  null"
				+ "  , memo        text          null"
				+ "  , create_dt   timestamp(0)  not null default current_timestamp"
				+ ")"
				+ "distributed randomly";
		
		stmt = con.createStatement();
		stmt.execute(sql);
		
		System.out.println("public.copy_manager_example 테이블 생성완료");
	}
	
	/**
	 * 기본적인 CSV 파일을 copyIn 으로 Import
	 * 
	 * @throws Exception
	 */
	public void importCsv() throws Exception {
		String csvFilePath = this.getClass().getResource("/wyleedp/greenplum/java/examples/GreenplumCopyManagerExample/1.csv").getPath();
		
		CopyManager copyManager = new CopyManager((BaseConnection)con);
		long copyInCount = copyManager.copyIn("COPY public.copy_manager_example FROM STDIN with CSV", new FileReader(csvFilePath));
		System.out.println("1.csv - copyInCount : " + copyInCount);
	}
	
	/**
	 * 쌍따옴표로 감싸져 있는 CSV 파일을 copyIn 으로 Import
	 * 
	 * @throws Exception
	 */
	public void importWrapperDoubleQuotationCsv() throws Exception {
		String csvFilePath = this.getClass().getResource("/wyleedp/greenplum/java/examples/GreenplumCopyManagerExample/2.csv").getPath();
		
		CopyManager copyManager = new CopyManager((BaseConnection)con);
		long copyInCount = copyManager.copyIn("COPY public.copy_manager_example FROM STDIN with CSV", new FileReader(csvFilePath));
		System.out.println("2.csv - copyInCount : " + copyInCount);
	}

	/**
	 * 쌍따옴표로 감싸져 있는 CSV 파일을 특정컬럼에만 Import
	 *   - create_dt 는 default가 current_timestamp 로 되어 있어 데이터가 INSERT 시점에 등록된다.
	 * 
	 * @throws Exception
	 */
	public void importWrapperDoubleQuotationCsvColumnMapping() throws Exception {
		String csvFilePath = this.getClass().getResource("/wyleedp/greenplum/java/examples/GreenplumCopyManagerExample/3.csv").getPath();

		CopyManager copyManager = new CopyManager((BaseConnection)con);
		long copyInCount = copyManager.copyIn("COPY public.copy_manager_example(no, title, memo) FROM STDIN with CSV", new FileReader(csvFilePath));
		System.out.println("3.csv - copyInCount : " + copyInCount);
	}
	
	/**
	 * public.copy_manager_example 테이블의 데이터를 CSV파일로 Export
	 * 
	 * @throws Exception 
	 */
	public void exportCsv() throws Exception {
		CopyManager copyManager = new CopyManager((BaseConnection)con);
		BufferedWriter bufferedWriter = null;

		try {
			// 윈도우 경로 예) C:\Users\<User명>\AppData\Local\Temp\export_160047.csv
			String csvFilePath = System.getProperty("java.io.tmpdir") + "export_"+DateFormatUtils.format(new Date(), "HHmmss")+".csv";
			bufferedWriter = new BufferedWriter(new FileWriter(csvFilePath), 1024);
			
			long copyOutCount = copyManager.copyOut("COPY (select * from public.test2) TO STDOUT WITH (FORMAT CSV, ENCODING 'UTF-8')", bufferedWriter);
			System.out.println("Export CsvFilePath : " + csvFilePath + ", CopyOutCount : " + copyOutCount);
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(bufferedWriter != null) {
				bufferedWriter.close();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		GreenplumCopyManagerExample example = new GreenplumCopyManagerExample();

		// 사전준비 - 예제테이블 삭제/생성(public.copy_manager_example)
		example.prepare();
		
		// 기본적인 CSV Import
		example.importCsv();
		
		// 쌍따폼표로 감싸져 있는 CSV Import
		example.importWrapperDoubleQuotationCsv();
		
		// 쌍따폼표로 감싸져 있는 CSV를 지정한 컬럼만 Import 
		example.importWrapperDoubleQuotationCsvColumnMapping();
		
		// public.copy_manager_example 테이블의 데이터를 CSV파일로 Export
		example.exportCsv();
		
		example.close();
	}
	
}
