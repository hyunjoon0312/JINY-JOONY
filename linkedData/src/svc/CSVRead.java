package svc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;

import com.opencsv.CSVReader;




public class CSVRead {


	public ArrayList<String[]> readCsv() {

		String filepath ="";
		Connection con=null;
		PreparedStatement pstmt4 = null;
		ResultSet rs = null;
		
		
		
		
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://112.72.158.187:3306/uploadFile", "hyunjoon",
					"hyunjoon");
			System.out.println("UploadDB connect success");
			
			
			pstmt4 = con.prepareStatement("SELECT filepath from uploadFile.UploadFileInfo ORDER BY uploadtime DESC limit 1");
			rs = pstmt4.executeQuery();
			if(rs.next()){
			filepath = rs.getString("filepath");}
			System.out.println(filepath);
			
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		ArrayList<String[]> data = new ArrayList<String[]>();

		try {
			// CSVReader reader = new CSVReader(new FileReader(filename), '\t');
			// UTF-8
			CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF-8"));
			String[] s;

			while ((s = reader.readNext()) != null) {
				data.add(s);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}



	public static ArrayList readCSV() {
		ArrayList<String[]> data = new ArrayList<String[]>();
		CSVRead read = new CSVRead();
		data = read.readCsv();
		Iterator<String[]> it = data.iterator();

		ArrayList<String> Jumin_array = new ArrayList<String>();

		while (it.hasNext()) {
			String[] array = (String[]) it.next();

			int i = 0;

			Jumin_array.add(array[i]);
			i += 1;

			// for (String s : array) {
			// System.out.print(s + " ");
			// }
			// System.out.print("\n");
		}

		return Jumin_array;
	}


}
