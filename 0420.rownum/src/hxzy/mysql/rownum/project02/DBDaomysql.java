package hxzy.mysql.rownum.project02;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DBDaomysql {

	private Connection conn;
	private PreparedStatement pstmt;

	public void connDB() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/hxzy", "root", "123456");
			System.out.println("数据库连接成功");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Object[][] sDBRow(int page, int row_no) {
		connDB();
		String sql = "select * from t_empinfo limit ?,?;";
		Object[][] batas = null;
		ResultSet rs;
		try {
			pstmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			pstmt.setInt(1, page - 1);
			pstmt.setInt(2, row_no);
			rs = pstmt.executeQuery();
			int count = 0;
			while (rs.next()) {
				count++;
			}

			rs.beforeFirst();
			// MetaDta指的是表头
			ResultSetMetaData rsmd = rs.getMetaData();
			// 获取数据表一共有多少列
			int lenght = rsmd.getColumnCount();
			batas = new Object[count][lenght];
			int row = 0;
			while (rs.next()) {
				for (int i = 0; i < lenght; i++) {
					batas[row][i] = rs.getString(i + 1);
				}
				row++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeDB();
		}
		return batas;
	}

	public void closeDB() {
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DBDaomysql db = new DBDaomysql();
		Object[][] batas = db.sDBRow(2, 2);

		for (int i = 0; i < batas.length; i++) {
			for (int k = 0; k < batas[i].length; k++) {
				System.out.print("\t" + batas[i][k]);
			}
			System.out.println("");
		}
	}

}
