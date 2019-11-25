import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Assignment {
    private String connection_url = "";
    private String username = "";
    private String password = "";
    private String driver="oracle.jdbc.driver.OracleDriver";
    private Connection connection = null;

    public Assignment(String connection_url, String username, String password){
        this.connection_url = connection_url;
        this.username = username;
        this.password = password;
        connectToDB();
    }

    private void connectToDB(){
        try
        {
            Class.forName(this.driver);
            this.connection = DriverManager.getConnection(this.connection_url, this.username, this.password);
            this.connection.setAutoCommit(false);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fileToDataBase(String path) {
        ArrayList<String[]> data = new ArrayList<String[]>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String new_line = "";
            while ((new_line = br.readLine()) != null) {
                String[] values = new_line.split(",");
                data.add(values);
            }

            String json = createJsonFileMoviesTable(data);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createJsonFileMoviesTable(ArrayList<String[]> data) {
        String json = "[ \n";

        for (int i = 0; i < data.size(); i++) {
            String[] movie_data = data.get(i);
            String line = "{ \n" +
                    "movie name: \"" + movie_data[0] + "\", year: \"" + movie_data[1] + "\"\n" +
                    "}, \n";
            json = json.concat(line);
        }

        json = json.concat("]");

        return json;
    }


//            String sqlQuery = "INSERT INTO films (movieName, year)\n" +
//                " SELECT movieName, year \n" +
//                " FROM OPENJSON(@" + json + ")";

    private void insertJsonToDB(String sqlQuery) {
        try {
            PreparedStatement ps = connection.prepareStatement(sqlQuery);
//            ps.set
			ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
			try{
				this.connection.rollback();
			}catch (SQLException e2) {
				e2.printStackTrace();
			}
			e.printStackTrace();
		} finally{
			try{
//				if(ps != null){
//					ps.close();
//				}
			}catch (Exception e3) {
				e3.printStackTrace();
			}
		}
    }

    public void calculateSimilarity(){
        String json = getMediaItems();
        String newJson = "[";
        Integer maxDistance = getMaxDistance();
        ArrayList<Long> MIDs = convertJsonToList(json);
        ArrayList<Long[]> MIDpairs = createMIDPair(MIDs);

        for (int i = 0; i < MIDpairs.size(); i++) {
               Long[] pair = MIDpairs.get(i);
               Float sim = getPairSimilarity(pair,maxDistance);
               String jsonItem = "{ \n" +
                       "MID1: \"" + pair[0].toString() + "\", MID2: \"" + pair[1] + "\", SIM: \""
                       + sim.toString() + "\" \n"
                       + "}, \n";
               newJson += jsonItem;
        }
        newJson += "]";
        String sqlQuery = "INSERT INTO similarty (MID1, MID2, sim)\n" +
                " SELECT MID1, MID2, sim \n" +
                " FROM OPENJSON(@" + newJson + ")";
        insertJsonToDB(sqlQuery);
    }

    private int getMaxDistance()
	{
	    return 10;
//		if(this.connection == null){
//			connectToDB();
//		}
//		int maxDistance = 0;
//		CallableStatement cs = null;
//		String call = "{? = call MaximalDistance()}"; //query
//		try{
//			cs = connection.prepareCall(call); //compiling query in the DB
//			cs.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
//			cs.execute();
//			maxDistance = cs.getInt(1);
//
//		}catch (SQLException e) {
//			e.printStackTrace();
//		}finally{
//			try{
//				if(cs != null){
//					cs.close();
//				}
//			}catch (Exception e3) {
//				e3.printStackTrace();
//			}
//		}
//		return maxDistance;
	}

    private String getMediaItems(){
        String sqlQuery = "SELECT * \n" +
        "FROM MediaItems \n" +
        "FOR JSON AUTO";
        try{
			PreparedStatement ps = connection.prepareCall(sqlQuery); //compiling query in the DB
			ps.execute();
			String json = ((CallableStatement) ps).getNString(1);

		}catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try{
//				if(cs != null){
//					cs.close();
//				}
			}catch (Exception e3) {
				e3.printStackTrace();
			}
		}
//		return json;
        return null;
    }

    private ArrayList<Long> convertJsonToList(String json){
        Matcher m = Pattern.compile("(?=(movie name))").matcher(json);
        ArrayList<Integer> pos = new ArrayList<>();
        while (m.find())
        {
            pos.add(m.start());
        }

        ArrayList<Long> MIDs = new ArrayList<>();
        for (int i = 0; i < pos.size(); i++) {
            Integer first_del = json.indexOf("\"", pos.get(i));
            Integer second_del = json.indexOf("\"", first_del + 1);
            Long MID = Long.parseLong(json.substring(first_del + 1, second_del));
            MIDs.add(MID);
        }
        return MIDs;
    }

    private ArrayList<Long[]> createMIDPair(ArrayList<Long> MIDs) {
        ArrayList<Long[]> pairs = new ArrayList<>();

        for (int i = 0; i < MIDs.size() - 1; i++) {
            Long[] pair = new Long[2];
            pair[0] = MIDs.get(i);
            pair[1] = MIDs.get(i + 1);
            pairs.add(pair);
        }

        return pairs;
    }

    private float getPairSimilarity(Long[] pair, Integer maxDistance) {


        return 0;
    }

    public void printSimilarItems(long mid)
	{
		ArrayList<String> similarMIDs = getSimilarMIDs(mid);
		for (String similarMID : similarMIDs)
		{
			System.out.println(similarMID);
		}
	}

	private ArrayList<String> getSimilarMIDs(long mid)
	{

		if(this.connection == null){
			connectToDB();
		}
		ArrayList<String> ans = new  ArrayList<String>();
		PreparedStatement ps = null;
		String query = "SELECT MEDIAITEMS.TITLE as TITLE,SIMILARITY.MID2,SIMILARITY.SIMILARITY as SIM FROM SIMILARITY " +
                "INNER JOIN MEDIAITEMS ON SIMILARITY.MID2=MEDIAITEMS.MID WHERE MID1=? ORDER BY SIMILARITY DESC";
		try{

			ps = connection.prepareStatement(query); //compiling query in the DB
			ps.setLong(1, mid);
			ResultSet rs=ps.executeQuery();
			while(rs.next()){
				if (rs.getDouble("SIM") >  0)
					ans.add(rs.getString("TITLE"));
			}
			rs.close();
		}catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try{
				if(ps != null){
					ps.close();
				}
			}catch (SQLException e3) {
				e3.printStackTrace();
			}
		}
		return ans;
	}

    public static void main(String[] args) {
        Assignment ass = new Assignment("1", "2", "3");

        ArrayList<String[]> list = new ArrayList<>();

        String test = "123, 23, 55";
        String[] test_arr = test.split(",");
        list.add(test_arr);
        test = "1111, 11, 25";
        test_arr = test.split(",");
        list.add(test_arr);

        String json = ass.createJsonFileMoviesTable(list);
        System.out.println(json);
        ArrayList<Long> MIDs = ass.convertJsonToList(json);
        System.out.println(MIDs);
    }
}


