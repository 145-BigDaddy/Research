import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.metrics.Updater;

public class Hbase_t {
	Connection con;
	Configuration conf;
	Admin admin;
	String db;
	boolean debug = true;

	public Hbase_t(String db) {
		conf = HBaseConfiguration.create();
		this.db = db;
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Cannot create connection");
			e.printStackTrace();
		}
		try {
			admin = ConnectionFactory.createConnection(conf).getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Cannot create admin");
			e.printStackTrace();
		}
	}

	void delete_table(HTableDescriptor tabledescriptor) {
		try {
			if (admin.tableExists(tabledescriptor.getTableName())) {
				admin.disableTable(tabledescriptor.getTableName());
				admin.deleteTable(tabledescriptor.getTableName());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to delete table");
			e.printStackTrace();
		}
	}

	void create_table(HTableDescriptor tabledescriptor) {
		try {
			if (admin.tableExists(tabledescriptor.getTableName())) {
				return;
			}
			admin.createTable(tabledescriptor);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(" Unable to create table");
			e.printStackTrace();
		}
	}

	HTableDescriptor create_tabledescriptor(String[] column_family,
			Boolean delete_db) {
		HTableDescriptor td = new HTableDescriptor(TableName.valueOf(db));
		if (delete_db)
			delete_table(td);

		for (String column : column_family) {
			HColumnDescriptor HColumn_Desc = new HColumnDescriptor(column);
			HColumn_Desc.setMaxVersions(1000000);
			td.addFamily(HColumn_Desc);

		}
		return td;

	}

	void put_data(String row, ArrayList<List<String>> data) {
		Table table = null;
		try {
			table = con.getTable(TableName.valueOf(db));
		} catch (IOException e) {
			System.out.println("Cannot create connection to table");
			// TODO Auto-generated catch block;
			e.printStackTrace();
			return;
		}
		Put put = new Put(Bytes.toBytes(row));

		for (List<String> list : data) {
			long time = System.nanoTime();
			if (debug)
				System.out.println("List values: " + row + " " + list.get(0)
						+ " " + list.get(1) + " " + list.get(2) + " ");
			put.addColumn(list.get(0).getBytes(), list.get(1).getBytes(), time,
					list.get(2).getBytes());
		}
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Table put is a failure");
			e.printStackTrace();
		}
		try {
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Table close is a failure");
			e.printStackTrace();
		}
		try {
			admin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Admin connection close is a failure");
			e.printStackTrace();
		}
	}

	public void insert_data_into_hbase(String cf, String cf_1, String key,
			String value) {
		ArrayList<List<String>> put_data_list = new ArrayList<>();

		List<String> stat_info = new ArrayList<String>();

		stat_info.add(cf);
		stat_info.add(cf_1);
		stat_info.add(value);
		put_data_list.add(stat_info);
		put_data(key, put_data_list);

	}

	public void update_from_file(String file) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Cannot buffer file");
			e.printStackTrace();

		}

		String Line = null;
		try {
			Line = br.readLine();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println(" Cannot read the line from buffer");
			e1.printStackTrace();
		}

		System.out.print("User input from file : " + Line);

		while (Line != null) {
			String sw_name = null;
			StringTokenizer st = new StringTokenizer(Line, ",");
			sw_name = st.nextToken();
			while (st.hasMoreTokens()) {

				String key = st.nextToken();
				if (key.contains("p")) {
					String module_no = st.nextToken();
					String row = sw_name + "/" + module_no;
					insert_data_into_hbase("power_stats", "maximum", row,
							st.nextToken());
					insert_data_into_hbase("power_stats", "current", row,
							st.nextToken());

				}

				if (key.contains("i")) {
					String module_no = st.nextToken();
					String row = sw_name + "/" + module_no;
					insert_data_into_hbase("interface_stats", "counter", row,
							st.nextToken());
				}

			}

			try {
				Line = br.readLine();
				System.out.println(Line);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("cannot read line");
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		Hbase_t hbase = new Hbase_t("analysis_db");
		String[] column = { "power_stats", "interface_stats" };
		HTableDescriptor table_descriptor = hbase.create_tabledescriptor(
				column, false);
		hbase.create_table(table_descriptor);
		hbase.update_from_file(args[0]);
		/*
		 * ArrayList<List<String>> data_list = new ArrayList<>(); String[] rows
		 * = { "xbow-1", "xbow-2", "xbow-3" }; for (int k = 0; k < rows.length;
		 * k++) {
		 * 
		 * for (int i = 0; i < 5; i++) { List<String> data = new ArrayList<>();
		 * data.add("power_stats"); data.add("maximum");
		 * data.add(String.valueOf(Integer.toString(Math .abs(10 * (new
		 * Random().nextInt()))))); data_list.add(data);
		 * 
		 * } for (int i = 0; i < 5; i++) { List<String> data = new
		 * ArrayList<>(); data.add("power_stats"); data.add("current");
		 * StringBuilder sb = new StringBuilder(); int value = (Math.abs(5 *
		 * (new Random().nextInt()))); sb.append(value);
		 * data.add(sb.toString()); data_list.add(data);
		 * 
		 * }
		 * 
		 * for (int i = 0; i < 5; i++) { List<String> data = new ArrayList<>();
		 * data.add("interface_stats"); data.add("counter"); int value =
		 * (Math.abs(100 * (new Random().nextInt()))); StringBuilder sb = new
		 * StringBuilder(); sb.append(value); data.add(sb.toString());
		 * data_list.add(data);
		 * 
		 * }
		 * 
		 * hbase.put_data(rows[k], data_list); }
		 */

	}

}
