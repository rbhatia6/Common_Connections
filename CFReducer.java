import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.commons.lang.StringUtils;


public class CFReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<String, Integer>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            String friends = iterator.next().toString();
            if (friends.equals("")) {
                context.write(key, new Text("[]"));
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        // now iterate the map to see how many have numOfValues
        List<String> commonFriends = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            //System.out.println(entry.getKey() + "/" + entry.getValue());
            if (entry.getValue() == numOfValues) {
               commonFriends.add(entry.getKey());
            }
        }
     
        // sen it to output
        context.write(key, new Text(commonFriends.toString()));
    }
   
    static void addFriends(Map<String, Integer> map, String friendsList) {
        String[] friends = StringUtils.split(friendsList, ",");
        for (String friend : friends) {
            Integer count = map.get(friend);
            if (count == null) {
                map.put(friend, 1);
            } 
            else {
                map.put(friend, ++count);
            }
        }
    }
}
